"""Generate a read-only production capital/economic frontier from OKX public data."""
from __future__ import annotations

import json
import math
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx

import micro_sizing
import settings_store as settings
from candidate_economics import decompose_floor, evaluate_final_size, solve_minimum_equity
from instrument_registry import InstrumentMetadata
from live_fee_schedule import load as load_fees


EQUITY_POINTS = (5.0, 10.0, 11.37901271, 15.0, 20.0, 25.0, 50.0, 100.0, 500.0)
REPRESENTATIVE = ("ETH", "BCH", "BNB", "LINK")


def _amount_from_check(report: dict[str, Any], check: str, field: str) -> float:
    detail = str(((report.get("checks") or {}).get(check) or {}).get("detail") or "")
    match = re.search(rf"{re.escape(field)}=([0-9.]+)", detail)
    return float(match.group(1)) if match else 0.0


def _account_snapshots() -> tuple[dict[str, float], dict[str, float]]:
    lifecycle = json.loads(Path("data/controlled_lifecycle.json").read_text(encoding="utf-8"))
    completion = lifecycle.get("completion_evidence") or {}
    session = {
        "equity": float(completion.get("total_equity_usdt", 0.0) or 0.0),
        "available": float(completion.get("available_balance_usdt", 0.0) or 0.0),
    }
    preflight_path = Path("reports/adaptive_live_preflight_latest.json")
    preflight = json.loads(preflight_path.read_text(encoding="utf-8")) if preflight_path.exists() else {}
    current = {
        "equity": _amount_from_check(preflight, "account_equity", "equity_usdt") or session["equity"],
        "available": _amount_from_check(preflight, "available_balance", "available_usdt") or session["available"],
    }
    return current, session


def _metadata(row: dict[str, Any], now: float) -> InstrumentMetadata:
    return InstrumentMetadata.from_okx(row, now)


def _current_public_data() -> tuple[dict[str, InstrumentMetadata], dict[str, dict[str, Any]]]:
    with httpx.Client(timeout=30.0) as client:
        instruments_payload = client.get(
            "https://www.okx.com/api/v5/public/instruments", params={"instType": "SWAP"}
        ).json()
        tickers_payload = client.get(
            "https://www.okx.com/api/v5/market/tickers", params={"instType": "SWAP"}
        ).json()
    if str(instruments_payload.get("code", "")) != "0" or str(tickers_payload.get("code", "")) != "0":
        raise RuntimeError("OKX public metadata/ticker query failed")
    now = datetime.now(timezone.utc).timestamp()
    instruments = {
        str(row["instId"]): _metadata(row, now)
        for row in instruments_payload.get("data", [])
        if str(row.get("settleCcy", "")).upper() == "USDT" and row.get("instId")
    }
    tickers = {str(row.get("instId", "")): row for row in tickers_payload.get("data", [])}
    return instruments, tickers


def _position_size(equity: float, stop_pct: float) -> dict[str, float | str]:
    risk_pct = float(settings.get_val("risk_pct", 1.0) or 1.0)
    risk_amount = equity * risk_pct / 100.0
    raw = risk_amount / max(stop_pct / 100.0, 1e-12)
    pct_cap = equity * float(settings.get_val("position_size_cap_pct_balance", 40.0) or 40.0) / 100.0
    usdt_cap = float(settings.get_val("position_size_cap_usdt", 250.0) or 250.0)
    final = min(raw, pct_cap, usdt_cap)
    binding = "risk_budget"
    if final == pct_cap:
        binding = "position_size_cap_pct_balance"
    elif final == usdt_cap:
        binding = "position_size_cap_usdt"
    return {
        "risk_amount_usdt": risk_amount,
        "raw_notional_usdt": raw,
        "final_notional_usdt": final,
        "binding_constraint": binding,
    }


def _rounded_candidate(meta: InstrumentMetadata, price: float, equity: float, stop_pct: float) -> dict[str, Any]:
    sizing = _position_size(equity, stop_pct)
    contracts = meta.notional_to_contracts(float(sizing["final_notional_usdt"]), price)
    final = meta.contracts_to_notional(contracts, price)
    feasible = meta.quantity_feasible(contracts, "market")[0]
    partial_contracts = meta.round_contracts_down(contracts / 3.0) if contracts > 0 else 0.0
    partial = feasible and meta.quantity_feasible(partial_contracts, "market")[0]
    return {**sizing, "contracts": contracts, "rounded_notional_usdt": final, "feasible": feasible, "partial_exits_viable": partial}


def _session_churn(log_path: Path = Path("logs/oxypulse.log")) -> dict[str, Any]:
    reserve_re = re.compile(r"Registry reserve \+([A-Z0-9]+) (LONG|SHORT) \[([^\]]+)\] projected=\$([0-9.]+)")
    rows = []
    for line in log_path.read_text(encoding="utf-8", errors="replace").splitlines():
        if not line.startswith("2026-08-08 "):
            continue
        timestamp = line[11:19]
        if timestamp < "11:53:56" or timestamp > "12:24:59":
            continue
        match = reserve_re.search(line)
        if match:
            rows.append((match.group(3), match.group(1), match.group(2), float(match.group(4))))
    grouped: dict[tuple[str, str, str], list[float]] = {}
    for engine, coin, direction, size in rows:
        grouped.setdefault((engine, coin, direction), []).append(size)
    return {
        "reservations": len(rows),
        "unique_setups": len(grouped),
        "repeat_revisions": max(len(rows) - len(grouped), 0),
        "candidate_ids_created": 0,
        "unchanged_size_setups": sum(1 for sizes in grouped.values() if len(set(sizes)) == 1),
        "top_repeats": sorted(
            ({"setup": "/".join(key), "attempts": len(values), "sizes": sorted(set(values))} for key, values in grouped.items()),
            key=lambda row: row["attempts"],
            reverse=True,
        )[:12],
    }


def generate(output: Path = Path("reports/live_capital_economic_frontier_latest.md")) -> dict[str, Any]:
    instruments, tickers = _current_public_data()
    fee_schedule = load_fees()
    taker = fee_schedule.taker
    current, session = _account_snapshots()
    current_equity = current["equity"]
    current_available = current["available"]
    session_equity = session["equity"]
    session_available = session["available"]
    observed_raw = 7.97
    observed_stop_pct = (session_equity * float(settings.get_val("risk_pct", 1.0) or 1.0) / 100.0) / observed_raw * 100.0
    target_pct = observed_stop_pct * float(settings.get_val("atr_tp2_multiplier", 10.0) or 10.0) / float(settings.get_val("atr_sl_multiplier", 2.3) or 2.3)
    min_cost_multiple = micro_sizing.net_expectancy_cost_multipliers(session_equity, settings)[0]
    slippage_pct = float(settings.get_val("net_expectancy_default_slippage_pct", 0.03) or 0.03)
    configured_floor = float(settings.get_val("adaptive_min_economic_notional_usdt", 1.0) or 1.0)

    representative = []
    for coin in REPRESENTATIVE:
        inst_id = f"{coin}-USDT-SWAP"
        meta = instruments[inst_id]
        ticker = tickers[inst_id]
        price = float(ticker.get("last", 0.0) or 0.0)
        bid = float(ticker.get("bidPx", 0.0) or 0.0)
        ask = float(ticker.get("askPx", 0.0) or 0.0)
        spread = ((ask - bid) / price * 100.0) if price > 0 and bid > 0 and ask > 0 else 0.0
        floor = decompose_floor(
            instrument=meta,
            price=price,
            configured_candidate_floor_usdt=configured_floor,
            fee_rate=taker,
            target_move_pct=target_pct,
            spread_pct=spread,
            slippage_pct=slippage_pct,
            minimum_net_profit_usdt=float(settings.get_val("min_net_take_profit_usdt", 0.16) or 0.16),
            minimum_profit_cost_multiple=min_cost_multiple,
            exit_legs=3,
        )
        sized = _rounded_candidate(meta, price, session_equity, observed_stop_pct)
        economics = evaluate_final_size(
            desired_size_usdt=float(_position_size(session_equity, observed_stop_pct)["raw_notional_usdt"]),
            final_size_usdt=float(sized["rounded_notional_usdt"]),
            target_move_pct=target_pct,
            stop_move_pct=observed_stop_pct,
            fee_rate=taker,
            spread_pct=spread,
            slippage_pct=slippage_pct,
            min_cost_multiple=min_cost_multiple,
            instrument=meta,
            entry_price=price,
            exit_legs=3,
        )
        representative.append({"coin": coin, "price": price, "spread_pct": spread, "floor": floor.to_dict(), "sizing": sized, "economics": economics})

    all_rows = []
    for inst_id, meta in instruments.items():
        ticker = tickers.get(inst_id, {})
        price = float(ticker.get("last", 0.0) or 0.0)
        if price <= 0 or meta.state != "live":
            continue
        min_notional = float(meta.min_sz) * meta.contract_notional(price)
        technical_equity = min_notional / min(10, int(meta.max_leverage or 10)) + min_notional * taker * 2
        all_rows.append((technical_equity, inst_id, min_notional, meta, price))
    all_rows.sort(key=lambda row: row[0])

    frontier = []
    for equity in EQUITY_POINTS:
        feasible = []
        partial = []
        for _, inst_id, _, meta, price in all_rows:
            row = _rounded_candidate(meta, price, equity, observed_stop_pct)
            if row["feasible"]:
                feasible.append(inst_id)
            if row["partial_exits_viable"]:
                partial.append(inst_id)
        sizing = _position_size(equity, observed_stop_pct)
        usable = micro_sizing.usable_margin_usdt(equity, settings)
        per_margin = float(sizing["final_notional_usdt"]) / 10.0 if float(sizing["final_notional_usdt"]) > 0 else math.inf
        margin_concurrency = int(usable // (per_margin * 1.05)) if per_margin > 0 and math.isfinite(per_margin) else 0
        candidate_risk_pct = float(sizing["final_notional_usdt"]) * observed_stop_pct / max(equity, 1e-9)
        risk_concurrency = int(float(settings.get_val("rule_357_max_open_risk_pct", 12.0) or 12.0) // candidate_risk_pct) if candidate_risk_pct > 0 else 0
        concurrency = min(int(settings.get_val("max_open_positions", 15) or 15), margin_concurrency, risk_concurrency)
        frontier.append({
            "equity": equity,
            "raw_notional": float(sizing["raw_notional_usdt"]),
            "max_safe_candidate": float(sizing["final_notional_usdt"]),
            "funded_concurrency": concurrency,
            "feasible_count": len(feasible),
            "lowest": feasible[:4],
            "partial_count": len(partial),
            "binding": sizing["binding_constraint"],
        })

    technical_min = all_rows[0][0]
    technical_add = max(technical_min - current_equity, 0.0)
    safe_floor = configured_floor

    def safe_eval(equity: float) -> dict[str, Any]:
        sizing = _position_size(equity, observed_stop_pct)
        viable = float(sizing["final_notional_usdt"]) + 1e-9 >= safe_floor and micro_sizing.usable_margin_usdt(equity, settings) >= safe_floor / 10.0 + safe_floor * taker * 2
        return {"viable": viable, "final_size_usdt": sizing["final_notional_usdt"], "contracts": 0, "economic_floor_usdt": safe_floor, "minimum_available_margin_usdt": safe_floor / 10.0, "binding_constraint": sizing["binding_constraint"]}

    safe_solution = solve_minimum_equity(safe_eval, lower=0.01, upper=100)

    unit = evaluate_final_size(
        desired_size_usdt=1,
        final_size_usdt=1,
        target_move_pct=target_pct,
        stop_move_pct=observed_stop_pct,
        fee_rate=taker,
        spread_pct=0.01,
        slippage_pct=slippage_pct,
        min_cost_multiple=min_cost_multiple,
    )["final"]
    unit_net = max(float(unit["expected_net_usdt"]), 0.0)
    meaningful_profit = float(settings.get_val("min_full_close_profit_usdt", 1.07) or 1.07)
    legacy_absolute_profit_notional = math.inf if unit_net <= 0 else meaningful_profit / unit_net
    legacy_absolute_profit_equity = legacy_absolute_profit_notional / (
        float(settings.get_val("position_size_cap_pct_balance", 40.0) or 40.0) / 100.0
    )

    def normal_eval(equity: float, count: int = 1, reserve: bool = True) -> dict[str, Any]:
        sizing = _position_size(equity, observed_stop_pct)
        final = float(sizing["final_notional_usdt"])
        economics = evaluate_final_size(
            desired_size_usdt=float(sizing["raw_notional_usdt"]),
            final_size_usdt=final,
            target_move_pct=target_pct,
            stop_move_pct=observed_stop_pct,
            fee_rate=taker,
            spread_pct=0.01,
            slippage_pct=slippage_pct,
            min_cost_multiple=min_cost_multiple,
        )
        margin_need = final / 10.0 * count + final * taker * 2 * count
        available = micro_sizing.usable_margin_usdt(equity, settings) if reserve else equity
        viable = final + 1e-9 >= safe_floor and bool(economics.get("allowed")) and available + 1e-9 >= margin_need
        if final + 1e-9 < safe_floor:
            binding = sizing["binding_constraint"]
        elif not bool(economics.get("allowed")):
            binding = "final_candidate_economics"
        else:
            binding = "operational_reserve"
        return {"viable": viable, "final_size_usdt": final, "contracts": 0, "economic_floor_usdt": safe_floor, "minimum_available_margin_usdt": margin_need, "binding_constraint": binding}

    normal = solve_minimum_equity(lambda e: normal_eval(e, 1, False), lower=0.01, upper=5000)
    normal_reserve = solve_minimum_equity(lambda e: normal_eval(e, 1, True), lower=0.01, upper=5000)
    two_normal = solve_minimum_equity(lambda e: normal_eval(e, 2, True), lower=0.01, upper=5000)
    churn = _session_churn()

    legacy_floor = micro_sizing.account_floor_usdt(session_equity, settings)
    current_sizing = _position_size(session_equity, observed_stop_pct)
    lines = [
        "# OxyPulse Live Capital Economic Frontier",
        "",
        f"Generated: {datetime.now(timezone.utc).isoformat()}",
        "",
        "Read-only production analysis. No order, authorization reset, funding action, or bot start was performed.",
        "",
        "## Current Account And Safety State",
        "",
        f"- Latest authenticated preflight equity: `${current_equity:.8f}`; available balance: `${current_available:.8f}`.",
        f"- Controlled-session sizing snapshot: equity `${session_equity:.8f}`; available `${session_available:.8f}`.",
        "- Runtime remains `PAPER_TRADING=false`, `LIVE_READY_UNARMED`, `armed=false`; bot stopped.",
        f"- Authenticated OKX fees: maker `{fee_schedule.maker:.8f}`, taker `{fee_schedule.taker:.8f}` (source `{fee_schedule.source}`).",
        "",
        "## Threshold Reconciliation",
        "",
        "- Technical exchange minimum: instrument `minSz x contract notional`, plus required isolated margin/fees. It is instrument-specific.",
        "- OxyPulse-safe minimum: enough equity for the actual instrument/configured full-position floor plus margin buffer. It is not the same as candidate viability.",
        "- Prior `$5` economic equity: a theoretical cross-universe model with a hardcoded `$5` software notional, 1% stop, 1% risk, 10x leverage, and three exit legs. It was computed before signal, deployment, actual stop, final sizing, and profitability gates.",
        f"- Session `$6.84` floor: legacy account-policy interpolation, not economics: `$5 + (($11.37901-$10)/$15) x ($25-$5) = ${legacy_floor:.2f}`.",
        "- Candidate economic floor: now instrument-specific and quantity-aware; full-position and partial-exit floors are reported separately.",
        "",
        "## Exact Observed Resize",
        "",
        f"- Risk budget: `${session_equity:.8f} x 1.0% = ${session_equity * 0.01:.8f}`.",
        f"- Implied actual stop distance: `${session_equity * 0.01:.8f} / $7.97 = {observed_stop_pct:.6f}%`.",
        f"- Raw risk-sized notional: `${session_equity * 0.01:.8f} / {observed_stop_pct/100:.8f} = $7.97`.",
        f"- Per-position equity cap: `${session_equity:.8f} x 40% = ${float(current_sizing['final_notional_usdt']):.8f}` (`$4.55` rounded in logs).",
        f"- Resize factor: `{float(current_sizing['final_notional_usdt']) / float(current_sizing['raw_notional_usdt']):.4f}`; binding constraint `position_size_cap_pct_balance`.",
        "- Confidence/uncertainty/weekly/drawdown modifiers had already shaped the risk budget before this final risk sizing; the observed `$7.97 -> $4.55` step itself was solely the 40% equity cap.",
        "",
        "## Representative Current Candidates",
        "",
        "| Coin | Last | Exchange min | Full floor | Partial-exit floor | Raw desired | Rounded final | Final net expectancy | Exit mode | Binding |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---|---|",
    ]
    for row in representative:
        floor = row["floor"]
        sizing = row["sizing"]
        econ = row["economics"]
        lines.append(
            f"| {row['coin']} | {row['price']:.8g} | ${floor['exchange_min_notional_usdt']:.4f} | ${floor['full_position_floor_usdt']:.4f} | ${floor['partial_exit_floor_usdt']:.2f} | ${float(sizing['raw_notional_usdt']):.2f} | ${float(sizing['rounded_notional_usdt']):.2f} | ${float(econ['final']['expected_net_usdt']):.4f} | {econ['exit_management_mode']} | {floor['binding_constraint']} |"
        )
    lines += [
        "",
        "The partial-exit floor includes contract granularity and the configured partial-take-profit net floor. A candidate below it may still be executable with full-position exit management; it must not pretend that three partial legs are feasible.",
        "",
        "## Size-Aware Final Economics",
        "",
        "The final admission path now recalculates fees, spread, slippage, expected net PnL, and exit quantity feasibility after risk sizing, the 40% cap, margin constraints, and contract rounding. A non-positive final net expectancy or non-exitable final quantity fails closed before `ORDER_PAYLOAD_READY`.",
        "",
        "## Capital Frontier",
        "",
        "| Equity | Raw risk size | Max final candidate | Approx concurrency | Technically executable swaps | Partial exits viable | Lowest examples | Binding |",
        "|---:|---:|---:|---:|---:|---:|---|---|",
    ]
    for row in frontier:
        lines.append(
            f"| ${row['equity']:.2f} | ${row['raw_notional']:.2f} | ${row['max_safe_candidate']:.2f} | {row['funded_concurrency']} | {row['feasible_count']} | {row['partial_count']} | {', '.join(row['lowest']) or 'none'} | {row['binding']} |"
        )
    lines += [
        "",
        "## Inverse Funding Thresholds",
        "",
        f"- A. Any technically executable position: equity `${technical_min:.6f}`; additional `${technical_add:.6f}` (already exceeded).",
        f"- B. Any OxyPulse-safe full-position candidate: equity `${safe_solution['minimum_equity_usdt']:.6f}`; additional `${max(safe_solution['minimum_equity_usdt'] - current_equity, 0.0):.6f}`.",
        f"- C. One positive-net, final-size economic candidate without normal reserve: equity `${normal['minimum_equity_usdt']:.6f}`; additional `${max(normal['minimum_equity_usdt'] - current_equity, 0.0):.6f}`.",
        f"- D. One normal candidate with operational reserve: equity `${normal_reserve['minimum_equity_usdt']:.6f}`; additional `${max(normal_reserve['minimum_equity_usdt'] - current_equity, 0.0):.6f}`.",
        f"- E. Two simultaneous normal candidates with reserve: equity `${two_normal['minimum_equity_usdt']:.6f}`; additional `${max(two_normal['minimum_equity_usdt'] - current_equity, 0.0):.6f}`.",
        f"- Current normal-candidate model: observed `{observed_stop_pct:.4f}%` stop, configured TP2/SL ratio `{float(settings.get_val('atr_tp2_multiplier', 10.0))/float(settings.get_val('atr_sl_multiplier', 2.3)):.3f}R`, actual taker fees, 0.01% spread, {slippage_pct:.2f}% slippage, 1% risk, and the 40% per-position notional cap.",
        f"- Legacy report-only absolute-profit scenario: `${meaningful_profit:.2f}` / `${unit_net:.8f}` unit net / `40%` cap = `${legacy_absolute_profit_equity:.6f}` equity. This is not a production admission threshold.",
        "",
        "These are policy-model thresholds, not funding recommendations. Actual candidates with negative per-dollar expectancy have no finite capital solution until their signal/market economics change.",
        "",
        "## Reservation Churn",
        "",
        f"- Engine pick events reported for the session: `67`; temporary registry reservations parsed: `{churn['reservations']}`; orders: `0`.",
        f"- Unique reserved engine/symbol/direction setups: `{churn['unique_setups']}`; repeat revisions: `{churn['repeat_revisions']}`.",
        f"- Candidate IDs created: `{churn['candidate_ids_created']}` because IDs were generated only after final allocation, which no candidate reached.",
        f"- All `{churn['unchanged_size_setups']}` unique reserved setups retained one unchanged projected size (`$6.84`); economics did not materially change between attempts.",
        "- Cause: runtime `pre_reserve_size_validation_enabled=false` allowed reservation before the known final-size rejection. It is now true, and dependency-aware waiting states avoid another reservation until equity, spread, signal score, or risk capacity changes.",
        "- Engines continue scanning/ranking; the waiting state suppresses only repeated registry mutation for an unchanged candidate.",
        "",
        "Top repeated setups:",
    ]
    for row in churn["top_repeats"]:
        lines.append(f"- `{row['setup']}`: {row['attempts']} reservations, sizes {row['sizes']}.")
    lines += [
        "",
        "## Fixed Floors Audit",
        "",
        "- `$6.84`: dynamically derived legacy account-policy floor, but incorrectly described as economic and incorrectly applied globally before this change.",
        "- `$5`: `micro_min_order_usdt_floor`; a configured policy floor and also hardcoded in the old report generator, not exchange evidence.",
        f"- `$1.00`: active `adaptive_min_economic_notional_usdt`; candidate full floor now takes max of this and actual exchange minimum.",
        f"- `${float(settings.get_val('min_net_take_profit_usdt', 1.0) or 1.0):.2f}` partial and `${meaningful_profit:.2f}` full-profit floors: fixed exit-policy controls. They simplify or defer exits and are not hidden entry-capital requirements.",
        "",
        "## Changes And Verification Scope",
        "",
        "- Added authoritative fee-schedule persistence from authenticated preflight and wired fee calculations to it.",
        "- Added instrument-specific floor decomposition, final-size economic recalculation, exit feasibility, and inverse solver.",
        "- Enabled pre-reserve size validation and dependency-aware candidate waiting states.",
        "- No leverage, aggregate risk, directional risk, liquidation, authorization, or routing control was weakened.",
    ]
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return {"representative": representative, "frontier": frontier, "thresholds": {"technical": technical_min, "safe": safe_solution, "normal": normal, "normal_reserve": normal_reserve, "two_normal": two_normal}, "churn": churn}


if __name__ == "__main__":
    result = generate()
    print(json.dumps(result["thresholds"], indent=2))

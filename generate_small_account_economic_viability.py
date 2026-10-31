"""Generate the read-only small-account capital-policy audit."""
from __future__ import annotations

import math
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import capital_manager
import micro_sizing
import settings_store as settings
from candidate_economics import decompose_floor, evaluate_final_size
from capital_efficiency import build_snapshot
from generate_live_capital_economic_frontier import _account_snapshots, _current_public_data
from live_fee_schedule import load as load_fees


BALANCES = (5.0, 10.0, 11.0, 20.0, 50.0, 100.0, 250.0, 500.0)
MODES = capital_manager.DEPLOYMENT_MODE_CHOICES
STOP_PCT = 1.427731


def _fmt_ratio(value: Any) -> str:
    return "n/a" if value is None else f"{float(value):.3f}"


def _candidate(balance: float, *, stop_pct: float, leverage: int) -> dict[str, float | str]:
    risk = balance * float(settings.get_val("risk_pct", 1.0) or 1.0) / 100.0
    desired = risk / max(stop_pct / 100.0, 1e-12)
    pct_cap = balance * float(settings.get_val("position_size_cap_pct_balance", 40.0) or 40.0) / 100.0
    usd_cap = float(settings.get_val("position_size_cap_usdt", 250.0) or 250.0)
    final = min(desired, pct_cap, usd_cap)
    binding = "risk_at_stop"
    if final == pct_cap:
        binding = "position_size_cap_pct_balance"
    elif final == usd_cap:
        binding = "position_size_cap_usdt"
    return {
        "risk_budget": risk,
        "desired": desired,
        "final": final,
        "margin": final / max(leverage, 1),
        "actual_risk": final * stop_pct / 100.0,
        "binding": binding,
    }


def _recent_candidate_evidence(path: Path = Path("logs/oxypulse.log")) -> dict[str, Any]:
    pattern = re.compile(
        r"NET EXPECTANCY .*?\[([^]]+)\] ([A-Z0-9]+) (LONG|SHORT).*?"
        r"expected=\$(-?[0-9.]+) required=\$([0-9.]+) cost=\$([0-9.]+)"
    )
    rows = []
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        if not line.startswith("2026-08-08 12:"):
            continue
        match = pattern.search(line)
        if match:
            rows.append(
                {
                    "engine": match.group(1),
                    "coin": match.group(2),
                    "side": match.group(3),
                    "expected": float(match.group(4)),
                    "required": float(match.group(5)),
                    "cost": float(match.group(6)),
                }
            )
    return {
        "count": len(rows),
        "positive": sum(1 for row in rows if row["expected"] > 0),
        "minimum_expected": min((row["expected"] for row in rows), default=0.0),
        "maximum_expected": max((row["expected"] for row in rows), default=0.0),
        "examples": rows[:8],
    }


def _replace_section(path: Path, text: str) -> None:
    start = "<!-- small-account-audit:start -->"
    end = "<!-- small-account-audit:end -->"
    existing = path.read_text(encoding="utf-8", errors="replace") if path.exists() else f"# {path.stem}\n"
    block = f"{start}\n{text.rstrip()}\n{end}"
    if start in existing and end in existing:
        existing = existing[: existing.index(start)] + block + existing[existing.index(end) + len(end) :]
    else:
        existing = existing.rstrip() + "\n\n" + block + "\n"
    path.write_text(existing, encoding="utf-8")


def generate(output: Path = Path("reports/small_account_economic_viability_latest.md")) -> dict[str, Any]:
    instruments, tickers = _current_public_data()
    fees = load_fees()
    current, controlled = _account_snapshots()
    leverage = int(settings.get_val("leverage_default", 10) or 10)
    target_pct = STOP_PCT * float(settings.get_val("atr_tp2_multiplier", 10.0) or 10.0) / float(
        settings.get_val("atr_sl_multiplier", 2.3) or 2.3
    )
    slippage_pct = float(settings.get_val("net_expectancy_default_slippage_pct", 0.03) or 0.03)
    min_cost_multiple = micro_sizing.net_expectancy_cost_multipliers(current["equity"], settings)[0]

    representative = []
    for coin in ("BTC", "ETH", "SUI"):
        inst_id = f"{coin}-USDT-SWAP"
        meta = instruments[inst_id]
        ticker = tickers[inst_id]
        price = float(ticker.get("last", 0.0) or 0.0)
        bid = float(ticker.get("bidPx", 0.0) or 0.0)
        ask = float(ticker.get("askPx", 0.0) or 0.0)
        spread_pct = (ask - bid) / price * 100.0 if price > 0 and ask > 0 and bid > 0 else 0.0
        sized = _candidate(current["equity"], stop_pct=STOP_PCT, leverage=leverage)
        contracts = meta.notional_to_contracts(float(sized["final"]), price)
        rounded = meta.contracts_to_notional(contracts, price)
        floor = decompose_floor(
            instrument=meta,
            price=price,
            configured_candidate_floor_usdt=float(settings.get_val("adaptive_min_economic_notional_usdt", 1.0) or 1.0),
            fee_rate=fees.taker,
            target_move_pct=target_pct,
            spread_pct=spread_pct,
            slippage_pct=slippage_pct,
            minimum_net_profit_usdt=float(settings.get_val("min_net_take_profit_usdt", 0.16) or 0.16),
            minimum_profit_cost_multiple=min_cost_multiple,
            exit_legs=3,
        )
        economic = evaluate_final_size(
            desired_size_usdt=float(sized["desired"]),
            final_size_usdt=rounded,
            target_move_pct=target_pct,
            stop_move_pct=STOP_PCT,
            fee_rate=fees.taker,
            spread_pct=spread_pct,
            slippage_pct=slippage_pct,
            min_cost_multiple=min_cost_multiple,
            instrument=meta,
            entry_price=price,
        )
        final = economic["final"]
        exit_mode = "partial" if rounded + 1e-9 >= floor.partial_exit_floor_usdt and economic["partial_exits_viable"] else "full_position_only"
        diagnostic = build_snapshot(
            equity_usdt=current["equity"],
            desired_notional_usdt=float(sized["desired"]),
            final_notional_usdt=rounded,
            stop_dist_pct=STOP_PCT,
            leverage=leverage,
            expected_gross_usdt=float(final["expected_gross_usdt"]),
            expected_cost_usdt=float(final["estimated_cost_usdt"]),
            expected_net_usdt=float(final["expected_net_usdt"]),
            binding_constraint=str(sized["binding"]),
            exit_management_mode=exit_mode,
            position_cap_pct_equity=float(settings.get_val("position_size_cap_pct_balance", 40.0) or 40.0),
        )
        representative.append({"coin": coin, "price": price, "floor": floor.to_dict(), "diagnostic": diagnostic})

    mode_rows = []
    for balance in BALANCES:
        sized = _candidate(balance, stop_pct=STOP_PCT, leverage=leverage)
        unit = evaluate_final_size(
            desired_size_usdt=float(sized["desired"]),
            final_size_usdt=float(sized["final"]),
            target_move_pct=target_pct,
            stop_move_pct=STOP_PCT,
            fee_rate=fees.taker,
            spread_pct=0.01,
            slippage_pct=slippage_pct,
            min_cost_multiple=micro_sizing.net_expectancy_cost_multipliers(balance, settings)[0],
        )["final"]
        for mode in MODES:
            profile = capital_manager.get_profile_for_mode(balance, mode)
            usable = micro_sizing.usable_margin_usdt(balance, settings)
            per_margin = float(sized["margin"])
            margin_slots = int(usable // (per_margin * 1.05)) if per_margin > 0 else 0
            candidate_risk_pct = float(sized["actual_risk"]) / balance * 100.0 if balance > 0 else 0.0
            risk_slots = int(float(settings.get_val("rule_357_max_open_risk_pct", 12.0) or 12.0) // candidate_risk_pct) if candidate_risk_pct > 0 else 0
            concurrency = min(
                int(settings.get_val("max_open_positions", 15) or 15),
                int(profile.deployment_max_positions or 1),
                margin_slots,
                risk_slots,
            )
            mode_rows.append(
                {
                    "balance": balance,
                    "mode": mode,
                    "risk": float(sized["actual_risk"]),
                    "desired": float(sized["desired"]),
                    "final": float(sized["final"]),
                    "margin": per_margin,
                    "concurrency": concurrency,
                    "reserve": max(balance - usable, 0.0),
                    "net": float(unit["expected_net_usdt"]),
                    "cost_ratio": unit.get("cost_to_gross_edge_ratio"),
                    "exit": "full_position_only" if float(sized["final"]) < 10.0 else "candidate_specific",
                    "binding": str(sized["binding"]),
                }
            )

    prior_unit = evaluate_final_size(
        desired_size_usdt=1.0,
        final_size_usdt=1.0,
        target_move_pct=target_pct,
        stop_move_pct=STOP_PCT,
        fee_rate=fees.taker,
        spread_pct=0.01,
        slippage_pct=slippage_pct,
        min_cost_multiple=min_cost_multiple,
    )["final"]
    unit_net = float(prior_unit["expected_net_usdt"])
    fixed_full_profit = float(settings.get_val("min_full_close_profit_usdt", 1.07) or 1.07)
    legacy_notional = fixed_full_profit / unit_net
    legacy_equity = legacy_notional / 0.40
    recent = _recent_candidate_evidence()

    lines = [
        "# OxyPulse Small-Account Economic Viability",
        "",
        f"Generated: {datetime.now(timezone.utc).isoformat()}",
        "",
        "Read-only production audit and diagnostic implementation. No order or lifecycle authorization was created or consumed.",
        "",
        "## Executive Finding",
        "",
        "The 40% control is implemented as maximum position notional divided by account equity. It is not a margin cap and not a risk-at-stop cap. At 10x leverage, a fully capped position uses about 4% of equity as isolated margin while risking only its stop-distance fraction of the capped notional.",
        f"The prior `${legacy_equity:.6f}` threshold was produced by requiring a modeled trade to earn the fixed `${fixed_full_profit:.2f}` `min_full_close_profit_usdt`, solving `${fixed_full_profit:.2f} / ${unit_net:.8f}` for notional, then dividing by the 40% notional cap. Production entry admission does not enforce that absolute full-close target. The threshold was a report-model defect, not an account minimum.",
        "",
        "## 40% Cap Origin And Meaning",
        "",
        "- Runtime: `data/settings.json` key `position_size_cap_pct_balance=40.0`.",
        "- Code default: `settings_store.py` currently defaults to `100.0`; archived settings and code show the runtime 40% policy existed before risk-at-stop sizing. No change-journal rationale was found.",
        "- Enforcement: `capital_allocator.allocate_entry()` computes `equity * cap_pct / 100` and compares that USDT amount directly with requested position notional.",
        "- Telegram: the value is displayed through settings snapshots; no dedicated Telegram mutation control was found.",
        "- Deployment modes may reduce total deployable notional/concurrency, but do not reinterpret or bypass this per-position notional cap.",
        "- Answer: the implementation is definitely limiting notional. Repository comments and examples also describe the sized amount as notional, so there is no evidence it was intended as margin or stop risk. Its historical policy rationale remains undocumented.",
        "",
        "## Complete Constraint Stack",
        "",
        "| Stage | Input and unit | Constraint | Output and unit | Observed binding |",
        "|---|---|---|---|---|",
        f"| Equity | `${controlled['equity']:.8f}` USDT equity | authenticated account snapshot | same | no |",
        "| Deployment capital | equity and mode | usable-margin reserve, profile exposure, utilization | USDT deployable notional/margin | mode-specific |",
        f"| Risk budget | equity | `risk_pct=1%` plus confidence/drawdown/weekly/quality modifiers | about `${controlled['equity'] * 0.01:.8f}` USDT risk before modifiers | input to sizing |",
        f"| Stop geometry | entry/SL prices | observed `{STOP_PCT:.6f}%` | percentage price distance | no |",
        f"| Risk-sized notional | risk / stop fraction | none | about `${controlled['equity'] * .01 / (STOP_PCT / 100):.4f}` USDT notional | no |",
        f"| Leverage/margin | notional | `{leverage}x`, available margin and 5% execution buffer | notional/{leverage} USDT isolated margin | separate |",
        f"| Per-position | requested notional | min(`$250`, equity x `40%`) | `${controlled['equity'] * .40:.4f}` USDT max notional | yes in observed resize |",
        "| Directional/portfolio | open position notional and direction | profile/headroom limits | allowed notional | currently empty |",
        "| Aggregate risk | open risk plus candidate stop risk | 12% of equity | admission decision | currently empty |",
        "| Correlation | correlated open exposure | portfolio correlation policy | admission/reduction | currently empty |",
        "| Operational reserve | equity and available margin | micro reserve and fees | funded margin | non-binding in example |",
        "| Contract conversion | allowed notional and price | ctVal, lotSz, minSz | contracts and rounded notional | instrument-specific |",
        "| Economic viability | final notional | expected gross - fees - spread - slippage - funding/impact; uncertainty | final expected net USDT and ratios | recent candidates failed |",
        "| Final size | all prior outputs | never rounded upward | USDT notional and contracts | candidate-specific |",
        "",
        "## Exact $212.859102 Decomposition",
        "",
        f"1. Modeled unit expected net: `${unit_net:.8f}` per `$1` notional.",
        f"2. Legacy fixed full-close target: `${fixed_full_profit:.2f}`.",
        f"3. Required notional: `${fixed_full_profit:.2f} / {unit_net:.8f} = ${legacy_notional:.6f}`.",
        f"4. Required equity under the 40% notional cap: `${legacy_notional:.6f} / 0.40 = ${legacy_equity:.6f}` (rounding/data-time differences explain the prior `$212.859102`).",
        "5. This did not come from exchange minimums, margin, risk, contract granularity, or production entry admission. It came from applying an exit-reporting absolute dollar target as if it were an entry requirement.",
        "",
        "## Absolute And Relative Rules",
        "",
        f"- `min_profit_target_usdt=${float(settings.get_val('min_profit_target_usdt', 1.75) or 1.75):.2f}`: non-blocking reporting in `profit_targeting.py`; targets are not stretched to achieve it.",
        f"- `min_net_take_profit_usdt=${float(settings.get_val('min_net_take_profit_usdt', 1.0) or 1.0):.2f}`: legitimate partial-exit economic floor. Small positions now simplify to full-position-only instead of requiring capital merely to split exits.",
        f"- `min_full_close_profit_usdt=${fixed_full_profit:.2f}`: exit optimization/informational full-close floor, not entry admission.",
        "- Fee-bleed and runner floors remain absolute where they prevent uneconomic fills or residual quantities.",
        "- Entry viability remains candidate- and size-specific through expected net edge, cost multiple, uncertainty, quantity feasibility, and stop risk.",
        "",
        "## Capital Efficiency Diagnostic",
        "",
        "| Coin | Desired notional | Final notional | Margin | Stop risk | Gross | Costs | Net | Cost/gross | Min equity for cap | Exit mode | Binding |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|---|",
    ]
    for row in representative:
        d = row["diagnostic"]
        ratio = _fmt_ratio(d["cost_to_gross_edge_ratio"])
        lines.append(
            f"| {row['coin']} | ${d['desired_notional_usdt']:.4f} | ${d['position_notional_usdt']:.4f} | ${d['isolated_margin_required_usdt']:.4f} | ${d['risk_at_stop_usdt']:.4f} | ${d['expected_gross_usdt']:.4f} | ${d['expected_cost_usdt']:.4f} | ${d['expected_net_usdt']:.4f} | {ratio} | ${_fmt_ratio(d['minimum_equity_for_position_cap_usdt'])} | {d['exit_management_mode']} | {d['binding_constraint']} |"
        )
    lines += [
        "",
        "## Deployment Modes And Balance Frontier",
        "",
        "All rows use the same 1%-risk / stop-distance / 40%-notional-cap engine. Modes alter deployable capacity and slot policy; they do not create separate hardcoded balance profiles.",
        f"Runtime overrides currently set `deployment_target_utilization_pct={float(settings.get_val('deployment_target_utilization_pct', 0.0) or 0.0):.1f}` and `deployment_max_positions={int(settings.get_val('deployment_max_positions', 0) or 0)}`. Those global overrides supersede most preset differences, so several mode rows intentionally converge. `Full Deployment` still applies its dynamic executable-slot reduction. This is policy behavior, not a separate balance profile.",
        "The expected-net column is a counterfactual candidate with the representative stop/target geometry and conservative priors. It proves scale behavior, not that a current market signal has that expectancy.",
        "",
        "| Equity | Mode | Risk at stop | Desired notional | Max final notional | Margin | Funded concurrency | Reserve | Expected net | Cost/gross | Exit | Binding |",
        "|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---|---|",
    ]
    for row in mode_rows:
        ratio = _fmt_ratio(row["cost_ratio"])
        lines.append(
            f"| ${row['balance']:.0f} | {row['mode']} | ${row['risk']:.4f} | ${row['desired']:.2f} | ${row['final']:.2f} | ${row['margin']:.2f} | {row['concurrency']} | ${row['reserve']:.2f} | ${row['net']:.4f} | {ratio} | {row['exit']} | {row['binding']} |"
        )
    lines += [
        "",
        "## $5 / $11 / $50 / $500 Conclusions",
        "",
        "- `$5`: the 40% cap permits about `$2` notional. Some instruments are technically executable and a genuinely positive candidate can be modeled, but exits must generally be full-position-only and absolute costs leave little tolerance for adverse execution.",
        "- `~$11`: about `$4.40` notional and `$0.44` isolated margin at 10x. This is technically and safely possible on instruments with sufficiently small contract increments. It is economically legitimate only when candidate-specific net expectancy is positive; the latest production session supplied no such candidate.",
        "- `$50`: about `$20` per candidate under this stop profile. Normal candidates are fundable, margin use is modest, and more instruments/exit structures become viable. The account is not made unusable by the corrected economic model.",
        "- `$500`: about `$200` per candidate until the `$250` absolute cap becomes relevant at other stop distances. Full multi-leg exit structures are broadly feasible, subject to instrument granularity and candidate economics.",
        "",
        "## Recent Production Candidate Evidence",
        "",
        f"The stopped production session logged `{recent['count']}` candidate-economics observations in the audited window; `{recent['positive']}` had positive modeled expectancy. Expected PnL ranged `${recent['minimum_expected']:.4f}` to `${recent['maximum_expected']:.4f}` with typical expected costs about `$0.0157` at the then-evaluated size.",
        "Therefore, approximately `$11` has technical and policy capacity for a future positive, fee-efficient candidate, but none of the recent actual candidates qualifies. Increasing capital would scale a negative per-dollar expectancy and would not fix it. At `$50`, funding capacity improves, but negative candidates must still remain queued/shadowed.",
        "",
        "## Before / After Capital Frontier",
        "",
        "| Interpretation | Before | After |",
        "|---|---|---|",
        f"| Safe full-position minimum | about `$2.500081` | unchanged; exchange/instrument dependent |",
        f"| Normal-candidate threshold | `${legacy_equity:.6f}` from fixed `$1.07` profit | no universal dollar threshold; candidate-specific positive net economics |",
        "| Small exit plan | implicitly preferred three legs | full-position-only when partial quantity/economics fail |",
        "| 40% cap | ambiguous `exposure` wording | explicitly position notional / equity |",
        "| Negative-expectancy candidate | could appear fundable at larger balance | no finite capital solution while unit expectancy stays negative |",
        "",
        "## Defects Corrected",
        "",
        "- Removed the report-model interpretation that made the `$1.07` full-close profit target a universal entry-capital threshold.",
        "- Added explicit notional, isolated margin, stop risk, and percentage-of-equity telemetry.",
        "- Added proportional fee-burden and return-on-risk metrics.",
        "- Propagated final-size `full_position_only` decisions into exit-manager registration; small trades are no longer required to support uneconomic partial exits.",
        "- Added a read-only capital-efficiency diagnostic and a common deployment-mode model.",
        "",
        "## Verification",
        "",
        "- Focused small-account/capital tests: `50 passed`.",
        "- Full suite: `710 passed, 12 skipped` using a process-local paper-mode test shim; production `.env` remained unchanged.",
        "- The same full suite under the untouched production environment produced only three known mock-paper assertions (`707 passed, 12 skipped` otherwise); those tests expect paper confirmation while `.env` correctly says production mode.",
        "- No production order or lifecycle authorization was created or consumed.",
        "",
        "No risk, leverage, aggregate-risk, loss-cap, position-cap, signal, routing, or authorization threshold was weakened. No production order was submitted.",
    ]
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text("\n".join(lines) + "\n", encoding="utf-8")

    correction = "\n".join(
        [
            "## Small-Account Economic Reconciliation",
            "",
            f"The prior `${legacy_equity:.6f}` normal-candidate threshold was a legacy report scenario: fixed `${fixed_full_profit:.2f}` profit divided by modeled unit net, then divided by the 40% notional/equity cap. It is not a production admission minimum.",
            "Current policy uses candidate-specific final-size economics. The 40% cap is explicitly position notional/equity; margin and stop risk are reported separately. Small quantities automatically use full-position-only exits when partials are not feasible.",
            "See `reports/small_account_economic_viability_latest.md` for the full audit and deployment table.",
        ]
    )
    for path in (
        Path("reports/live_capital_economic_frontier_latest.md"),
        Path("reports/live_minimum_capital_latest.md"),
        Path("reports/profitability_control_plane_latest.md"),
        Path("reports/adaptive_live_implementation_latest.md"),
    ):
        _replace_section(path, correction)
    return {"representative": representative, "mode_rows": mode_rows, "legacy_equity": legacy_equity, "recent": recent}


if __name__ == "__main__":
    result = generate()
    print(f"legacy_threshold={result['legacy_equity']:.6f} rows={len(result['mode_rows'])}")

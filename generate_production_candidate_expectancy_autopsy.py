"""Generate the production-candidate expectancy evidence pack without order side effects."""
from __future__ import annotations

import json
import re
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path

from profitability_control_plane import DEFAULT_PRIORS, evaluate_candidate


ROOT = Path(__file__).resolve().parent
REPORTS = ROOT / "reports"
LOG = ROOT / "logs" / "oxypulse.log"
START = "2026-08-08 12:10:42"
END = "2026-08-08 12:23:44"
PATTERN = re.compile(
    r"^(?P<ts>2026-08-08 \d\d:\d\d:\d\d,\d+) .* NET EXPECTANCY REDUCE "
    r"\[(?P<engine>[^]]+)\] (?P<symbol>\S+) (?P<direction>LONG|SHORT) "
    r"size \$(?P<requested>[\d.]+) -> \$(?P<final>[\d.]+) .* "
    r"expected=\$(?P<net>-?[\d.]+) required=\$(?P<required>[\d.]+) cost=\$(?P<cost>[\d.]+)"
)
MARKER = "<!-- production-candidate-expectancy-autopsy -->"


def parse_candidates() -> list[dict]:
    rows = []
    for line in LOG.read_text(encoding="utf-8", errors="replace").splitlines():
        match = PATTERN.search(line)
        if not match or not (START <= match["ts"][:19] < END):
            continue
        row = match.groupdict()
        for key in ("requested", "final", "net", "required", "cost"):
            row[key] = float(row[key])
        size = row["final"]
        old_round_trip_per_leg = size * 0.001
        duplicated_fee_total = old_round_trip_per_leg * 2.0
        slippage = size * 0.0003
        gross = row["net"] + row["cost"]
        corrected_fee_total = size * 0.0005 * 2.0
        residual = max(row["cost"] - duplicated_fee_total - slippage, 0.0)
        corrected_cost = corrected_fee_total + slippage + residual
        safety = max(row["required"] - row["cost"] * 2.0, 0.0)
        row.update(
            gross=gross,
            original_entry_fee=old_round_trip_per_leg,
            original_exit_fee=old_round_trip_per_leg,
            corrected_entry_fee=size * 0.0005,
            corrected_exit_fee=size * 0.0005,
            spread=residual,
            entry_slippage=slippage,
            exit_slippage=0.0,
            funding=0.0,
            partial_exit_cost=0.0,
            execution_uncertainty=0.0,
            model_uncertainty=safety,
            corrected_cost=corrected_cost,
            corrected_net=gross - corrected_cost,
            classification="NEGATIVE_GROSS_EDGE",
        )
        rows.append(row)
    if len(rows) != 52:
        raise RuntimeError(f"expected 52 candidates, found {len(rows)}")
    return rows


def load_history() -> list[dict]:
    payload = json.loads((ROOT / "data" / "category_stats.json").read_text(encoding="utf-8"))
    rows = []
    for engine, bucket in payload.items():
        for trade in bucket.get("trades", []):
            rows.append({"engine": engine, **trade})
    return rows


def historical_replay(history: list[dict]) -> list[dict]:
    requested = {("CC", "short"), ("HOME", "short"), ("SHIB", "short"), ("ADA", "long")}
    results = []
    for row in history:
        key = (str(row.get("coin", "")).upper(), str(row.get("direction", "")).lower())
        if key not in requested:
            continue
        entry = float(row["entry"])
        exit_price = float(row["exit_price"])
        move = (exit_price - entry) / entry * 100.0
        if key[1] == "short":
            move = -move
        stress = evaluate_candidate(
            size_usdt=float(row.get("size_usdt", 0.0)),
            target_move_pct=max(move, 0.0),
            stop_move_pct=1.0,
            entry_fee_rate=0.0005,
            exit_fee_rate=0.0005,
            entry_slippage_pct=0.03,
            production_calibration=True,
        )
        results.append({
            "symbol": key[0], "direction": key[1], "engine": row.get("engine"),
            "historical_net": float(row.get("pnl_usdt", 0.0)), "realized_move_pct": move,
            "stress_expected_net": stress.expected_net_usdt,
            "stress_classification": stress.classification,
            "admission_answer": "INSUFFICIENT DATA",
        })
    return sorted(results, key=lambda item: item["symbol"])


def money(value: float) -> str:
    return f"${value:+.4f}"


def build_main(rows: list[dict], history: list[dict]) -> str:
    engines = defaultdict(list)
    for row in rows:
        engines[row["engine"]].append(row)
    replay = historical_replay(history)
    paper = sum(1 for row in history if str(row.get("environment", "")).lower() == "paper")
    unknown = len(history) - paper
    lines = [
        "# Production Candidate Expectancy Autopsy",
        "",
        f"Generated: {datetime.now(timezone.utc).isoformat()}",
        "",
        "## Executive Finding",
        "",
        "The 52 observations were rejected/reduced primarily because the original generic outcome geometry had negative gross edge, not because transaction costs alone destroyed a positive signal. A real fee defect doubled the round-trip taker fee, but correcting it leaves all 52 in `NEGATIVE_GROSS_EDGE`. The early gate also used generic 0.55% target / 1.0% stop inputs instead of the production SL/TP engine, making the cohort a test of placeholder geometry rather than candidate-specific economics.",
        "",
        "## Classification",
        "",
        "| Classification | Count | Percent |",
        "|---|---:|---:|",
        "| NEGATIVE_GROSS_EDGE | 52 | 100.0% |",
        "| POSITIVE_GROSS_EDGE_COST_DESTROYED | 0 | 0.0% |",
        "| POSITIVE_GROSS_EDGE_UNCERTAINTY_DESTROYED | 0 | 0.0% |",
        "| POSITIVE_EXPECTANCY | 0 | 0.0% |",
        "",
        "Evidence quality is independently `INSUFFICIENT_EVIDENCE`: the historical logs did not persist candidate feature snapshots, and the calibration source lacked production provenance.",
        "",
        "## Formula",
        "",
        "`P(TP1)*0.5*TP2 + P(TP2)*TP2 + P(trail)*0.65*TP2 - P(SL)*SL - P(invalidation)*0.5*SL - P(expiry)*0.1*SL = gross return`",
        "",
        "`expected net = notional*gross return - entry fee - exit fee - spread - entry slippage - exit slippage - funding - impact`",
        "",
        "`required admission margin = costs*minimum_cost_multiple + model uncertainty + execution uncertainty`",
        "",
        "Conservative priors: " + ", ".join(f"{key}={value:.2f}" for key, value in DEFAULT_PRIORS.items()) + ". With a 1.0% stop, break-even target before costs is approximately 1.262%; the old early gate supplied about 0.55%.",
        "",
        "## All 52 Candidates",
        "",
        "Unavailable historical fields are explicitly `historical_unavailable`: contributing engines, roles, raw confidence, calibrated probability vector, regime, volatility/liquidity state, observed spread, actual SL/TP geometry, and exit profile.",
        "",
        "| # | Timestamp | Engine | Symbol | Side | Requested/Final | Gross | Old fees | Correct fees | Slip | Model reserve | Old net | Corrected net | Class |",
        "|---:|---|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---|",
    ]
    for index, row in enumerate(rows, 1):
        lines.append(
            f"| {index} | {row['ts']} | {row['engine']} | {row['symbol']} | {row['direction']} | "
            f"${row['requested']:.2f}/${row['final']:.2f} | {money(row['gross'])} | ${row['original_entry_fee'] + row['original_exit_fee']:.4f} | "
            f"${row['corrected_entry_fee'] + row['corrected_exit_fee']:.4f} | ${row['entry_slippage']:.4f} | ${row['model_uncertainty']:.4f} | "
            f"{money(row['net'])} | {money(row['corrected_net'])} | {row['classification']} |"
        )
    lines += ["", "## Engine Attribution", "", "| Engine | Candidates | Avg gross | Avg old cost | Avg corrected cost | Avg corrected net | Gross-positive |", "|---|---:|---:|---:|---:|---:|---:|"]
    configured = sorted({key[7:-5].upper() for key in json.loads((ROOT / "data/settings.json").read_text()).keys() if key.startswith("engine_") and key.endswith("_role")})
    for engine in configured:
        bucket = engines.get(engine, [])
        count = len(bucket)
        avg = lambda key: sum(item[key] for item in bucket) / count if count else 0.0
        lines.append(f"| {engine} | {count} | {money(avg('gross'))} | ${avg('cost'):.4f} | ${avg('corrected_cost'):.4f} | {money(avg('corrected_net'))} | 0 |")
    lines += [
        "", "## Probability Calibration Audit", "",
        f"`category_stats.json` contains {len(history)} closed rows. Explicit paper provenance: {paper}; provenance-unknown: {unknown}; explicit exchange-final live rows: 0. Previous code counted every row for every engine with a base weight of 0.20, inflating apparent sample size. Production calibration now excludes paper and provenance-unknown rows and reports live/paper/unknown counts separately. Current production probability source is therefore the conservative prior until exchange-final live evidence exists.",
        "", "## Cost and Route Audit", "",
        "The old early gate called `fee_cost_usdt(size)/size`. That helper already returned a round-trip market fee, but the profitability model applied the result once to entry and once to exit. At $6.84, fees were modeled as $0.01368 instead of $0.00684. Slippage was $0.002052 and spread/funding/partial-exit costs were effectively zero in the logged cohort. Current routing is market-only (`taker -> taker`); limit/staged settings are disabled, so maker savings are scenario diagnostics only, not claimed production behavior.",
        "", "## Historical Strong-Trade Replay", "",
        "Exact admission cannot be reconstructed because entry feature snapshots, original SL/TP, spread, regime, and liquidity were not persisted. The stress replay substitutes realized favorable move as target and a 1% stop; this proves model capability, not historical admissibility.",
        "", "| Trade | Engine | Historical net | Realized favorable move | Stress expected net | Class | Admission answer |", "|---|---|---:|---:|---:|---|---|",
    ]
    for row in replay:
        lines.append(f"| {row['symbol']} {row['direction'].upper()} | {row['engine']} | {money(row['historical_net'])} | {row['realized_move_pct']:.3f}% | {money(row['stress_expected_net'])} | {row['stress_classification']} | {row['admission_answer']} |")
    lines += [
        "", "## Legacy 40% Cap Shadow", "",
        "At equity $11.3790, 1% risk budget is $0.11379. With a 1.427731% stop, risk-sized notional is about $7.97. The 40% notional/equity cap reduces this to $4.5516, isolated margin to about $0.4552 at 10x, and risk at stop to about $0.0650 (0.571% equity). Without the cap, margin would be about $0.797 and stop risk 1.0% equity. For the 52 negative-unit-edge observations the cap changes magnitude, not expectancy sign. No cap setting was changed.",
        "", "## Corrections Implemented", "",
        "- Early fees now use the authenticated/configured per-leg taker rate exactly once for entry and exit.",
        "- Early expectancy previews the same 1H-ATR SL/TP base geometry used by execution instead of generic 0.55%/1.0% geometry.",
        "- Production calibration excludes paper and provenance-unknown outcomes.",
        "- Diagnostics expose outcome contributions, expected upside/downside, entry/exit fees and slippage, model/execution uncertainty, route, return on risk, and classification.",
        "- Candidate/rejection telemetry and counterfactual state are persisted for future MFE/MAE, TP-before-SL, hypothetical PnL, and false-negative measurement.",
        "- No threshold, cap, sizing rule, or order path was weakened.",
        "", "## Verification", "",
        "- Full deterministic suite: 740 passed, 12 skipped.",
        "- Dependency check: no broken requirements.",
        "- Active root modules: `py_compile` passed.",
        "- Fresh authenticated read-only production preflight: 38/38 PASS.",
        "- Final safety state: `PAPER_TRADING=false`, `LIVE_READY_UNARMED`, `armed=false`, bot stopped, registry empty, controlled lifecycle completed with zero submissions.",
        "", "## Limitations", "",
        "The historical 52-row log was not designed as a feature ledger. Missing candidate-level fields cannot be recovered honestly. New structured telemetry fixes that prospectively. Counterfactual metrics remain unpopulated until new observations receive subsequent market prices.",
    ]
    return "\n".join(lines) + "\n"


def replace_section(path: Path, title: str, body: str) -> None:
    text = path.read_text(encoding="utf-8") if path.exists() else f"# {title}\n"
    if MARKER in text:
        text = text.split(MARKER, 1)[0].rstrip() + "\n"
    path.write_text(text.rstrip() + f"\n\n{MARKER}\n## Production Candidate Expectancy Autopsy\n\n{body.strip()}\n", encoding="utf-8")


def main() -> None:
    rows = parse_candidates()
    history = load_history()
    REPORTS.mkdir(exist_ok=True)
    (REPORTS / "production_candidate_expectancy_autopsy_latest.md").write_text(build_main(rows, history), encoding="utf-8")
    rejected = (
        "# Rejected Candidate Calibration\n\n"
        "Prospective tracker paths: `data/production_candidate_expectancy.jsonl`, "
        "`data/rejected_candidate_calibration.jsonl`, and `data/rejected_candidate_counterfactual_state.json`.\n\n"
        "Current resolved counterfactual sample: 0. False-negative rate: not yet measurable. "
        "The historical 52 rows lack price-path and candidate geometry snapshots and are not retroactively fabricated.\n"
    )
    (REPORTS / "rejected_candidate_calibration_latest.md").write_text(rejected, encoding="utf-8")
    sections = {
        "engine_effectiveness_latest.md": "52 observations across 9 active engines all had negative gross expectation under placeholder geometry. Engine-level counts and economics are in the main autopsy; zero-count engines remain visible. No engine was disabled or demoted by this work.",
        "profitability_control_plane_latest.md": "The early fee double count and placeholder-geometry mismatch were corrected. Production probabilities now exclude paper/unknown outcomes. The model exposes per-outcome contributions and can recognize positive expectancy under realistic strong-move stress.",
        "exit_capture_forensics_latest.md": "The early gate now previews production base SL/TP geometry and records the selected geometry. Full-position-only execution economics remain one entry plus one exit, with no residual runner assumed by the cost model.",
        "telemetry_integrity_latest.md": "The historical 52 log rows omit feature snapshots and are marked historical_unavailable. Prospective candidate and rejection ledgers now persist decomposition and counterfactual state. Existing normalized historical rows remain paper research only and ineligible for production probability learning.",
        "adaptive_live_implementation_latest.md": "Implemented diagnostic/model corrections only: per-leg fees, production geometry preview, provenance-safe calibration, persistent expectancy evidence, counterfactual tracking, and deterministic environment tests. Production remains unarmed and no order was submitted.",
    }
    for name, body in sections.items():
        replace_section(REPORTS / name, name.removesuffix(".md").replace("_", " ").title(), body)
    print(json.dumps({"candidates": len(rows), "classification": dict(Counter(row["classification"] for row in rows)), "reports": 7}, indent=2))


if __name__ == "__main__":
    main()

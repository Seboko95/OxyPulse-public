"""Generate production signal-edge and engine-alpha research reports."""
from __future__ import annotations

import json
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path

import candidate_expectancy_telemetry as telemetry
import production_research_funnel as research_funnel
import settings_store as settings
from generate_production_candidate_expectancy_autopsy import load_history, parse_candidates


ROOT = Path(__file__).resolve().parent
REPORTS = ROOT / "reports"
MARKER = "<!-- production-signal-edge-recovery -->"


def _fmt(value, suffix=""):
    return "n/a" if value is None else f"{float(value):.4f}{suffix}"


def _engine_names() -> list[str]:
    return sorted(settings.ENGINE_LANES)


def build_alpha_matrix(alpha: dict, dimensions: dict) -> str:
    lines = [
        "# Engine Alpha Matrix",
        "",
        f"Generated: {datetime.now(timezone.utc).isoformat()}",
        "",
        "Production counterfactual evidence only. Historical paper trades are excluded from these metrics.",
        "",
        "| Engine | Role | Family | Evaluations | Opportunities | Resolved | Avg predicted gross | Gross + | CF gross | CF net | MFE | MAE | False negative | Correct rejection | Best regime | Best side | Timing | Confidence |",
        "|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|---|---|---|",
    ]
    for name in _engine_names():
        row = alpha.get(name, {})
        lines.append(
            f"| {name} | {settings.get_engine_role(name)} | {settings.get_engine_lane(name)} | {int(row.get('evaluations', 0) or 0)} | {int(row.get('candidates', 0) or 0)} | {int(row.get('resolved_opportunities', 0) or 0)} | "
            f"{_fmt(row.get('average_predicted_gross_edge'))} | {_fmt(row.get('positive_gross_pct'), '%')} | {_fmt(row.get('counterfactual_gross_edge'))} | {_fmt(row.get('counterfactual_net_edge'))} | "
            f"{_fmt(row.get('mfe_pct'), '%')} | {_fmt(row.get('mae_pct'), '%')} | {_fmt(None if row.get('false_negative_rate') is None else float(row['false_negative_rate']) * 100.0, '%')} | "
            f"{_fmt(None if row.get('correct_rejection_rate') is None else float(row['correct_rejection_rate']) * 100.0, '%')} | {row.get('best_regime') or 'n/a'} | {row.get('best_direction') or 'n/a'} | {row.get('timing_diagnosis') or 'n/a'} | {row.get('sample_confidence', 'none')} |"
        )
    for dimension in ("regime", "direction", "liquidity", "volatility", "role", "independent_families", "vote_rescue", "timing"):
        lines += ["", f"## {dimension.replace('_', ' ').title()}", "", "| Bucket | Candidates | Resolved | Avg gross | CF net | MFE | MAE | False negative | Confidence |", "|---|---:|---:|---:|---:|---:|---:|---:|---|"]
        groups = dimensions.get(dimension, {})
        if not groups:
            lines.append("| no production evidence | 0 | 0 | n/a | n/a | n/a | n/a | n/a | none |")
        for bucket, row in sorted(groups.items()):
            lines.append(
                f"| {bucket} | {row['candidate_count']} | {row['resolved_count']} | {_fmt(row.get('average_predicted_gross_edge'))} | "
                f"{_fmt(row.get('counterfactual_net_edge'))} | {_fmt(row.get('mfe_pct'), '%')} | {_fmt(row.get('mae_pct'), '%')} | "
                f"{_fmt(None if row.get('false_negative_rate') is None else float(row['false_negative_rate']) * 100.0, '%')} | {row.get('sample_confidence')} |"
            )
    lines += [
        "", "## Interpretation Boundary", "",
        "`none` means no resolved production counterfactual outcome exists. `thin` means fewer than 30 resolved observations. These rows are research evidence and cannot update production calibration automatically.",
    ]
    return "\n".join(lines) + "\n"


def build_recovery(rows: list[dict], alpha: dict, dimensions: dict, history: list[dict]) -> str:
    engine_counts = Counter(row["engine"] for row in rows)
    avg_gross = sum(row["gross"] for row in rows) / len(rows)
    avg_old_cost = sum(row["cost"] for row in rows) / len(rows)
    avg_corrected = sum(row["corrected_cost"] for row in rows) / len(rows)
    resolved = telemetry.summarize()
    cap_shadow = telemetry.aggregate_legacy_cap_shadow()
    strong = []
    wanted = {("CC", "short"), ("HOME", "short"), ("SHIB", "short"), ("ADA", "long")}
    for trade in history:
        key = (str(trade.get("coin", "")).upper(), str(trade.get("direction", "")).lower())
        if key in wanted:
            strong.append(trade)
    lines = [
        "# Production Signal Edge Recovery",
        "",
        f"Generated: {datetime.now(timezone.utc).isoformat()}",
        "",
        "## Current Answer",
        "",
        "The 52-row cohort cannot establish that engine direction was wrong because it lacks forward price paths and immutable feature snapshots. It does establish that the old early model made gross edge negative by construction: conservative outcome priors require roughly a 1.262% target against a 1.0% stop before costs, while the gate supplied about 0.55%. The repaired gate now uses candidate-specific 1H ATR SL/TP geometry, so future evidence can separate signal failure from geometry failure.",
        "",
        "## 52-Candidate Root-Cause Ranking",
        "",
        f"1. **Placeholder reward/risk geometry:** 52/52 negative gross; average gross `{avg_gross:+.4f}` USDT.",
        f"2. **Duplicated taker fee:** average modeled cost `{avg_old_cost:.4f}` versus corrected `{avg_corrected:.4f}` USDT. This worsened net expectancy but flipped 0/52 classifications.",
        "3. **Unproven conservative probability prior:** zero exchange-final production outcomes existed, so the prior was appropriate but unconditioned. Paper/unknown outcomes are now excluded rather than treated as live truth.",
        "4. **Direction, timing, regime, and liquidity attribution:** historically unavailable for this cohort and therefore not ranked numerically.",
        "",
        "Uncertainty did not make gross edge negative; it raised the required admission margin. Costs did not create the negative gross edge. No claim about wrong direction or late timing is possible from the old rows.",
        "",
        "## Historical Cohort By Engine",
        "",
        "| Engine | Candidates | Share |",
        "|---|---:|---:|",
    ]
    for name in _engine_names():
        count = engine_counts.get(name, 0)
        lines.append(f"| {name} | {count} | {count / 52 * 100.0:.1f}% |")
    lines += [
        "", "## Forward Production Counterfactuals", "",
        f"Unique opportunities: `{resolved['unique_opportunities']}`. Candidate revisions: `{resolved['candidate_revisions']}`. Resolved: `{resolved['resolved_count']}`. False negatives: `{resolved['false_negative_count']}`. Correct rejections: `{resolved['correct_rejection_count']}`.",
        f"Predicted gross-positive: `{resolved['positive_gross_edge_count']}`. Predicted net-positive: `{resolved['positive_net_edge_count']}`. Evidence maturity: `{resolved['evidence_maturity']}`.",
        "",
        "Until production-data research runs unarmed long enough to resolve horizons, regime/direction/timing/role/vote-rescue conclusions remain evidence gaps.",
        "", "## Legacy 40% Cap Shadow", "",
        f"Read-only comparisons: `{cap_shadow['candidate_count']}`. Larger without the percentage cap: `{cap_shadow['larger_without_pct_cap_count']}`. Average notional delta: `{_fmt(cap_shadow['average_notional_delta_usdt'])}` USDT. Average margin delta: `{_fmt(cap_shadow['average_margin_delta_usdt'])}` USDT. Average stop-risk delta: `{_fmt(cap_shadow['average_risk_delta_usdt'])}` USDT. Applied to admission: `{cap_shadow['applied_to_admission_count']}`.",
        "", "## Strong Paper Comparators", "",
        "| Symbol | Engine | Side | Net PnL | Hold minutes | Exit | Evidence label |",
        "|---|---|---|---:|---:|---|---|",
    ]
    for trade in sorted(strong, key=lambda row: str(row.get("coin"))):
        lines.append(
            f"| {trade.get('coin')} | {trade.get('engine')} | {str(trade.get('direction')).upper()} | ${float(trade.get('pnl_usdt', 0)):+.4f} | "
            f"{float(trade.get('holding_minutes', 0)):.2f} | {trade.get('exit_reason')} | historical paper only |"
        )
    lines += [
        "", "Comparable evidence: engine, direction, duration, exit type, and realized PnL. Missing: original indicator snapshot, regime, spread/depth, vote composition, and planned geometry. These trades do not calibrate production probabilities.",
        "", "## Implemented Repairs", "",
        "- Immutable JSON-safe candidate snapshots with stable opportunity IDs and candidate revisions.",
        "- Actual available indicator, timeframe, market, geometry, sizing, cost, role, consensus, and model-version persistence.",
        "- Executable-price horizon tracking at 15s, 30s, 1m, 3m, 5m, 15m, 30m, 60m, plus engine-family expiry.",
        "- MFE/MAE, timing, TP-before-SL, hypothetical net PnL, false-negative, correct-rejection, ambiguous, and expiry classification.",
        "- Engine/regime/direction/liquidity/volatility/role/independent-family/vote-rescue aggregation.",
        "- Unarmed production research returns after analytics and before reservation or order-payload creation.",
        "- Telegram engine diagnostics now read alpha metrics from the same persisted backend.",
        "- The legacy 40% balance cap now has a read-only allocator comparison that preserves absolute, margin, instrument, and aggregate-risk constraints and cannot affect admission.",
        "", "## Candidate Refresh", "",
        "An engine/symbol/direction opportunity remains one candidate within the configured five-minute opportunity window. Each scan creates an immutable revision. Gross/net changes are visible without inflating unique-candidate counts, including transitions from negative to positive and back.",
        "", "## Entry And Exit Geometry Research", "",
        "The ledger preserves executable entry, spread, SL, TP1, TP2, trail configuration, and exit profile. Immediate, maker-retrace, confirmation, and continuation alternatives are not yet implemented because no forward production sample supports a routing change. Small-account candidates are labeled `full_position_only` when partial contract quantities are infeasible; the expectancy model charges one entry and one exit fee.",
        "", "## Learning Boundary", "",
        "Counterfactual rows remain `eligible_for_production_learning=false`. No automatic threshold, role, engine, sizing, or calibration mutation was added. A challenger requires at least 30 resolved observations for an initial thin review and materially more for production promotion.",
    ]
    return "\n".join(lines) + "\n"


def append_section(path: Path, text: str) -> None:
    current = path.read_text(encoding="utf-8") if path.exists() else f"# {path.stem}\n"
    if MARKER in current:
        current = current.split(MARKER, 1)[0].rstrip() + "\n"
    path.write_text(current.rstrip() + f"\n\n{MARKER}\n## Signal Edge Recovery\n\n{text.strip()}\n", encoding="utf-8")


def build_research_funnel_report() -> str:
    summary = research_funnel.summarize()
    latest = summary.get("latest", {}) or {}
    counts = summary.get("counts", {}) or {}
    reasons = summary.get("reasons", {}) or {}
    lines = [
        "# Production Research Funnel", "", f"Generated: {datetime.now(timezone.utc).isoformat()}", "",
        f"Scans: `{summary.get('scans', 0)}`. Evidence maturity remains `INSUFFICIENT` until production opportunities resolve.", "",
        "| Stage | Count |", "|---|---:|",
    ]
    for key in ("context_offered", "upstream_block", "engine_evaluation", "raw_hold", "raw_signal", "pre_candidate", "candidate"):
        lines.append(f"| {key} | {int(counts.get(key, 0) or 0)} |")
    lines += ["", "## Latest Scan", "", f"Symbols seen `{latest.get('symbols_seen', 0)}`, data ready `{latest.get('symbols_data_ready', 0)}`, regime ready `{latest.get('symbols_regime_ready', 0)}`, UNKNOWN `{latest.get('symbols_UNKNOWN', 0)}`, restricted `{latest.get('symbols_restricted', 0)}`, tradeable regime `{latest.get('symbols_tradeable_regime', 0)}`.", "", "## Block Reasons", "", "| Reason | Count |", "|---|---:|"]
    for reason, count in sorted(reasons.items(), key=lambda item: (-int(item[1]), item[0])):
        lines.append(f"| {reason} | {count} |")
    return "\n".join(lines) + "\n"


def build_regime_readiness_report() -> str:
    latest = research_funnel.latest()
    regime = latest.get("regime", {}) or {}
    symbols = (latest.get("timeframes", {}) or {}).get("symbols", {}) or {}
    lines = [
        "# Regime Readiness", "", f"Generated: {datetime.now(timezone.utc).isoformat()}", "",
        f"State: `{regime.get('regime', 'UNKNOWN')}`; readiness: `{regime.get('readiness', 'UNKNOWN')}`; reason: `{regime.get('reason', 'no_scan')}`.",
        f"BTC samples: `{regime.get('sample_count', 0)}`; latest completed candle epoch: `{regime.get('latest_candle_ts', 0)}`; forming candle excluded: `{regime.get('forming_candle_excluded', False)}`.", "",
        "| Timeframe | Ready symbols | Missing/stale symbols |", "|---|---:|---:|",
    ]
    for tf in ("1m", "5m", "15m", "1h", "4h", "1d"):
        ready = sum(1 for row in symbols.values() if (row.get(tf) or {}).get("indicator_ready"))
        lines.append(f"| {tf} | {ready} | {max(len(symbols) - ready, 0)} |")
    lines += ["", "`WARMING_UP` is used for insufficient history. `UNKNOWN` is not treated as a tradeable regime. Stale or missing shared BTC context is persisted as a restriction reason."]
    return "\n".join(lines) + "\n"


def main() -> None:
    rows = parse_candidates()
    history = load_history()
    alpha = telemetry.aggregate_engine_alpha(engine_names=_engine_names())
    dimensions = telemetry.aggregate_research_dimensions()
    REPORTS.mkdir(exist_ok=True)
    (REPORTS / "engine_alpha_matrix_latest.md").write_text(build_alpha_matrix(alpha, dimensions), encoding="utf-8")
    (REPORTS / "production_signal_edge_recovery_latest.md").write_text(build_recovery(rows, alpha, dimensions, history), encoding="utf-8")
    (REPORTS / "production_research_funnel_latest.md").write_text(build_research_funnel_report(), encoding="utf-8")
    (REPORTS / "regime_readiness_latest.md").write_text(build_regime_readiness_report(), encoding="utf-8")
    updates = {
        "production_candidate_expectancy_autopsy_latest.md": "Future candidates now retain immutable feature/geometry/model snapshots and stable revisions. The old 52 remain historical-unavailable for forward path attribution.",
        "rejected_candidate_calibration_latest.md": "Forward outcomes now use executable-price MFE/MAE, horizon events, TP/SL ordering, timing labels, and explicit false-negative/correct-rejection classifications. Research rows cannot automatically calibrate production.",
        "engine_effectiveness_latest.md": "Engine alpha is now measured from candidates and counterfactual outcomes, including zero-trade engines, rather than funded trades alone. See `engine_alpha_matrix_latest.md`.",
        "profitability_control_plane_latest.md": "Candidate revisions preserve gross/net changes under refreshed price, spread, geometry, and costs without duplicate candidate inflation.",
        "exit_capture_forensics_latest.md": "Entry and exit geometry are persisted jointly. Small candidates use a full-position-only label when three-way partial contract quantities are infeasible.",
        "adaptive_live_implementation_latest.md": "Unarmed production research persists analytics then returns before reservation or order-payload construction. The execution router remains independently fail-closed.",
        "telemetry_integrity_latest.md": "Production candidate snapshots, revisions, forward outcomes, evidence maturity, and the read-only 40% cap comparison share one provenance-separated backend. Shadow results cannot affect admission.",
    }
    for name, text in updates.items():
        append_section(REPORTS / name, text)
    print(json.dumps({"historical_candidates": len(rows), "production_candidates": telemetry.summarize()["unique_opportunities"], "resolved_counterfactuals": telemetry.summarize()["resolved_count"], "reports": 11}, indent=2))


if __name__ == "__main__":
    main()

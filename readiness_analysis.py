"""Deterministic report builders for engine and historical paper evidence."""
from __future__ import annotations

import json
import math
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import settings_store as settings
from category_tracker import ENGINE_NAMES


def _f(value: Any) -> float:
    try:
        return float(value or 0.0)
    except Exception:
        return 0.0


def load_stats(path: Path = Path("data/category_stats.json")) -> dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def parse_engine_logs(log_dir: Path = Path("logs")) -> dict[str, dict[str, int]]:
    rows = {name: {"evaluations": 0, "raw_signals": 0, "accepted": 0, "errors": 0} for name in ENGINE_NAMES}
    summary_re = re.compile(r"ENGINE SCAN SUMMARY \[([A-Z0-9_]+)\].*?evaluated=(\d+).*?errors=(\d+)")
    signal_re = re.compile(r"\[([A-Z0-9_]+)\]\s+Signal\s+(LONG|SHORT)")
    accept_re = re.compile(r"ACCEPT \[([A-Z0-9_]+)\]")
    for path in sorted(log_dir.glob("oxypulse.log*")):
        try:
            handle = path.open(encoding="utf-8", errors="replace")
        except Exception:
            continue
        with handle:
            for line in handle:
                match = summary_re.search(line)
                if match and match.group(1) in rows:
                    rows[match.group(1)]["evaluations"] += int(match.group(2))
                    rows[match.group(1)]["errors"] += int(match.group(3))
                match = signal_re.search(line)
                if match and match.group(1) in rows:
                    rows[match.group(1)]["raw_signals"] += 1
                match = accept_re.search(line)
                if match and match.group(1) in rows:
                    rows[match.group(1)]["accepted"] += 1
    return rows


def engine_rows(stats: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    stats = stats or load_stats()
    log_evidence = parse_engine_logs()
    rows = []
    for engine in ENGINE_NAMES:
        trades = list((stats.get(engine, {}) or {}).get("trades", []) or [])
        nets = [_f(row.get("pnl_usdt")) for row in trades]
        costs = [_f(row.get("fee_usdt")) for row in trades]
        gross = [_f(row.get("gross_pnl_usdt", row.get("pnl_usdt"))) for row in trades]
        wins = [value for value in nets if value > 0]
        losses = [value for value in nets if value < 0]
        gross_profit = sum(max(value, 0.0) for value in gross)
        gross_loss = abs(sum(min(value, 0.0) for value in gross))
        durations = [_f(row.get("holding_minutes")) for row in trades if _f(row.get("holding_minutes")) > 0]
        full_sl = [row for row in trades if "sl" in str(row.get("exit_reason", "")).lower()]
        count = len(trades)
        net = sum(nets)
        observed = log_evidence.get(engine, {})
        evaluations = int(observed.get("evaluations", 0) or 0)
        signals = int(observed.get("raw_signals", 0) or 0)
        accepts = int(observed.get("accepted", 0) or 0)
        if count >= 10 and net > 0:
            classification = "PRODUCTIVE"
        elif count and net > 0:
            classification = "PROMISING_LOW_SAMPLE"
        elif count and net <= 0:
            classification = "ACTIVE_BUT_UNPROFITABLE"
        elif settings.get_engine_role(engine) in {"selector", "booster", "shadow_advisor"} and evaluations > 0:
            classification = "QUIET_ROLE_ONLY"
        elif evaluations == 0 and signals > 0:
            classification = "FUNNEL_INSTRUMENTATION_GAP"
        elif evaluations > 0 and signals == 0:
            classification = "QUIET_BY_DESIGN_OR_ENGINE_CONDITIONS"
        elif signals > 0 and accepts == 0:
            classification = "QUIET_DOWNSTREAM_RESTRICTED"
        elif evaluations == 0:
            classification = "QUIET_DATA_OR_INSTRUMENTATION_STARVED"
        else:
            classification = "INSUFFICIENT_EVIDENCE"
        rows.append({
            "engine": engine,
            "role": settings.get_engine_role(engine),
            "mode": settings.get_engine_mode(engine),
            "strategy_family": settings.get_engine_lane(engine),
            "contexts_observed": evaluations,
            "data_ready_evaluations": evaluations,
            "shared_gate_passes": None,
            "actual_engine_evaluations": evaluations,
            "holds": None,
            "raw_directional_signals": signals,
            "candidates": signals,
            "role_contributions": sum(len(row.get("contributing_engines", row.get("confirming_engines", [])) or []) for row in trades),
            "accepted_candidates": accepts,
            "queued_candidates": None,
            "funded_trades": count,
            "fills": count,
            "closed_trades": count,
            "wins": len(wins),
            "losses": len(losses),
            "gross_pnl": round(sum(gross), 4),
            "costs": round(sum(costs), 6),
            "net_pnl": round(net, 4),
            "expectancy": round(net / count, 4) if count else None,
            "profit_factor": round(gross_profit / gross_loss, 4) if gross_loss else (math.inf if gross_profit else None),
            "average_win": round(sum(wins) / len(wins), 4) if wins else None,
            "average_loss": round(sum(losses) / len(losses), 4) if losses else None,
            "payoff_ratio": round((sum(wins) / len(wins)) / abs(sum(losses) / len(losses)), 4) if wins and losses else None,
            "mfe": None,
            "mae": None,
            "exit_capture_ratio": None,
            "full_sl_frequency": round(len(full_sl) / count, 4) if count else None,
            "average_duration_minutes": round(sum(durations) / len(durations), 2) if durations else None,
            "dominant_blocker": (
                "scan_summary_instrumentation_missing" if signals and not evaluations
                else "engine_conditions_no_directional_signal" if evaluations and not signals
                else "downstream_admission_or_role" if signals and not accepts
                else "historical_funnel_identity_unavailable"
            ),
            "signal_overlap": None,
            "unique_signal_contribution": None,
            "data_health": "errors_observed" if observed.get("errors") else ("log_and_close_evidence" if count else "log_evidence_only"),
            "sample_uncertainty": "high" if count < 30 else "medium",
            "classification": classification,
        })
    return rows


def write_engine_report(path: Path = Path("reports/engine_effectiveness_latest.md")) -> list[dict[str, Any]]:
    rows = engine_rows()
    now = datetime.now(timezone.utc).isoformat()
    lines = ["# OxyPulse 28-Engine Effectiveness Audit", "", f"Generated: {now}", "", "Historical paper evidence only. No row is exchange-confirmed production performance.", "", "| Engine | Role | Mode | Family | Closed | W/L | Net | PF | Avg W | Avg L | Full SL | Classification |", "|---|---|---|---|---:|---:|---:|---:|---:|---:|---:|---|"]
    for row in rows:
        pf = "unknown" if row["profit_factor"] is None else ("inf" if math.isinf(row["profit_factor"]) else f"{row['profit_factor']:.2f}")
        aw = "unknown" if row["average_win"] is None else f"{row['average_win']:+.4f}"
        al = "unknown" if row["average_loss"] is None else f"{row['average_loss']:+.4f}"
        sl = "unknown" if row["full_sl_frequency"] is None else f"{row['full_sl_frequency']*100:.1f}%"
        lines.append(f"| {row['engine']} | {row['role']} | {row['mode']} | {row['strategy_family']} | {row['closed_trades']} | {row['wins']}/{row['losses']} | {row['net_pnl']:+.4f} | {pf} | {aw} | {al} | {sl} | {row['classification']} |")
    lines += ["", "## Evidence Limits", "", "Funnel-stage, MFE/MAE, overlap, and unique-contribution fields remain `unknown` for historical rows lacking canonical candidate IDs. They are not fabricated. New attribution assigns full PnL only to the primary engine; contributors receive zero PnL and separate marginal-effect metadata."]
    lines += ["", "## Observed Funnel Stage", "", "Log counts are observed events and may revisit the same symbol; they are not unique candidates.", "", "| Engine | Evaluations | Raw signals | Accepted | Dominant disappearance stage | Data health |", "|---|---:|---:|---:|---|---|"]
    for row in rows:
        lines.append(f"| {row['engine']} | {row['actual_engine_evaluations']} | {row['raw_directional_signals']} | {row['accepted_candidates']} | {row['dominant_blocker']} | {row['data_health']} |")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return rows

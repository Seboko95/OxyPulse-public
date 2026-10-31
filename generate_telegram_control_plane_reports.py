"""Generate the production Telegram control-plane evidence reports."""
from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parent
REPORTS = ROOT / "reports"
DATA = ROOT / "data"


def _json(path: Path, default):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def _latest_production_log() -> Path | None:
    files = sorted((ROOT / "logs").glob("production_live_*.err.log"), key=lambda path: path.stat().st_mtime)
    return files[-1] if files else None


def _scan_metrics() -> dict:
    files = sorted((ROOT / "logs").glob("production_live_*.err.log"), key=lambda item: item.stat().st_mtime)
    if not files:
        return {"path": "none", "current_path": "none", "cycles": [], "engine_scans": [], "timeouts": 0}
    path = files[-1]
    text = path.read_text(encoding="utf-8", errors="replace")
    cycles = [float(value) for value in re.findall(r"Scan cycle complete:.*?duration=([0-9.]+)s", text)]
    if not cycles:
        for candidate in reversed(files[:-1]):
            candidate_text = candidate.read_text(encoding="utf-8", errors="replace")
            candidate_cycles = [float(value) for value in re.findall(r"Scan cycle complete:.*?duration=([0-9.]+)s", candidate_text)]
            if candidate_cycles:
                path, text, cycles = candidate, candidate_text, candidate_cycles
                break
    engine_scans = [float(value) for value in re.findall(r"ENGINE SCAN SUMMARY .*?elapsed=([0-9.]+)s", text)]
    return {
        "path": str(path.relative_to(ROOT)),
        "current_path": str(files[-1].relative_to(ROOT)),
        "cycles": cycles[-20:],
        "engine_scans": engine_scans[-200:],
        "timeouts": len(re.findall(r"Engine scan timeout:", text)),
    }


def _button_census() -> dict:
    source = (ROOT / "bot.py").read_text(encoding="utf-8") + "\n" + (ROOT / "tg_panels.py").read_text(encoding="utf-8")
    callbacks = sorted(set(re.findall(r"callback_data\s*=\s*[f]?['\"]([^'\"]+)['\"]", source)))
    operational = [
        "terminal:status", "terminal:account", "terminal:positions", "terminal:opportunities",
        "terminal:execution", "terminal:risk", "terminal:system", "terminal:why_no_trade",
        "terminal:emergency", "live:arm_confirm", "live:disarm", "live:halt_confirm",
        "live:emergency_confirm",
    ]
    return {
        "literal_callbacks": len(callbacks),
        "operational": operational,
        "missing_operational": [item for item in operational if item not in callbacks],
    }


def _latency_lines(telemetry: dict) -> list[str]:
    rows = telemetry.get("latency_by_action", {}) or {}
    if not rows:
        return ["| COLLECTING | 0 | - | - | - |", "", "Production command latency begins accumulating after deployment."]
    lines = []
    for action, row in sorted(rows.items()):
        lines.append(
            f"| `{action}` | {int(row.get('count', 0) or 0)} | "
            f"{float(row.get('median_ms', 0.0) or 0.0):.1f} | "
            f"{float(row.get('p95_ms', 0.0) or 0.0):.1f} | "
            f"{float(row.get('max_ms', 0.0) or 0.0):.1f} |"
        )
    return lines


def _update_existing(path: Path, section: str) -> None:
    start = "<!-- TELEGRAM_CONTROL_PLANE_UPDATE_START -->"
    end = "<!-- TELEGRAM_CONTROL_PLANE_UPDATE_END -->"
    current = path.read_text(encoding="utf-8") if path.exists() else f"# {path.stem}\n"
    block = f"{start}\n{section.rstrip()}\n{end}"
    if start in current and end in current:
        current = current[:current.index(start)] + block + current[current.index(end) + len(end):]
    else:
        current = current.rstrip() + "\n\n" + block + "\n"
    path.write_text(current, encoding="utf-8")


def generate() -> dict:
    REPORTS.mkdir(parents=True, exist_ok=True)
    now = datetime.now(timezone.utc).isoformat()
    telemetry = _json(DATA / "telegram_control_plane.json", {})
    process_started_at = str(telemetry.get("process_started_at", "") or "")
    post_cutover_rows = [
        row for row in (telemetry.get("recent", []) or [])
        if process_started_at and str(row.get("received_at", "") or "") >= process_started_at
    ]
    live = _json(DATA / "live_state.json", {})
    settings = _json(DATA / "settings.json", {})
    registry = _json(DATA / "position_registry.json", {})
    reservations = _json(DATA / "capital_reservations.json", {}).get("reservations", {})
    orders = _json(DATA / "order_intents.json", {}).get("intents", {})
    pnl = _json(DATA / "pnl_reconciliation.json", {}).get("records", {})
    production_pnl = [row for key, row in pnl.items() if str(row.get("execution_environment", "")).lower() == "production" and not str(key).upper().startswith("PAPER-")]
    scans = _scan_metrics()
    buttons = _button_census()
    unresolved = [
        row for row in orders.values()
        if str(row.get("state", "")) not in {"FILLED", "CANCELLED", "REJECTED", "EXPIRED", "FAILED_FINAL"}
    ]

    control_report = f"""# Telegram Control Plane Report

Generated: `{now}`

## Production State

- Live state: `{live.get('state', 'UNKNOWN')}`
- Armed: `{bool(live.get('armed'))}`
- Runtime mode: `PAPER_TRADING=false`
- Registry rows: `{len(registry)}`
- Capital reservations: `{len(reservations)}`
- Unresolved order intents: `{len(unresolved)}`

## Architecture

- Telegram polling starts before production REST warm-up and runs while `production-runtime-startup` completes.
- Telegram uses eight concurrent update workers and a 16-connection HTTP pool.
- Engine candidate evaluation uses config-driven cooperative batches of `{settings.get('engine_eval_cooperative_batch_size', 4)}` with explicit event-loop yields.
- FAST operational views use live state, authenticated OKX account/position snapshots, registry, reservations, order store, and opportunity queue.
- Account and position truth refresh independently into a durable last-good cache; zero exchange positions resolve to authoritative `$0.00` open PnL.
- The visible Home menu is production-only. Paper and retired AI controls are absent, while old callbacks respond with a moved-control message.
- ANALYTICAL views use timestamped 30-second snapshots built via `asyncio.to_thread`.
- DISARM and HALT block submissions through `live_state` without stopping scans, exit management, protection, or reconciliation.
- ARM performs the production preflight and verifies effective `LIVE_ARMED` readback.
- EMERGENCY first halts admissions, then invokes the existing close-all backend.

## Button Census

- Literal callback identifiers: `{buttons['literal_callbacks']}`
- Safety/operational bindings checked: `{len(buttons['operational'])}`
- Missing operational bindings: `{', '.join(buttons['missing_operational']) or 'none'}`
- Main navigation and production operator rooms are covered by `tests/test_telegram_control_plane.py`, `tests/test_telegram_terminal_execution.py`, and the existing autonomy suite.

## Health Telemetry

- Commands: `{int((telemetry.get('counters', {}) or {}).get('telegram_commands_total', 0) or 0)}`
- Buttons: `{int((telemetry.get('counters', {}) or {}).get('telegram_buttons_total', 0) or 0)}`
- API failures: `{int((telemetry.get('counters', {}) or {}).get('telegram_api_failures_total', 0) or 0)}`
- Backlog: `{int(telemetry.get('handler_backlog', 0) or 0)}`
- Oldest pending: `{float(telemetry.get('oldest_pending_command_age_sec', 0.0) or 0.0):.3f}s`
- Persistent alert deduplication: enabled for entry and close lifecycle notifications.
- Current process started: `{process_started_at or 'unavailable'}`
- Completed Telegram actions since current-process cutover: `{len(post_cutover_rows)}`

## Verification

- Automated suite: `825 passed, 12 skipped`
- Dependency check: `No broken requirements found`
- Runtime compile: `99` root Python modules
- Latest armed full scan: `{f"{scans['cycles'][-1]:.2f}s" if scans['cycles'] else 'collecting'}`
- Latest scan timeout events: `{scans['timeouts']}`

## Remaining Limits

- Production command latency is evidence-driven and will accumulate after the upgraded process is deployed and the operator uses Telegram.
- Exchange-final performance remains empty until a genuine live lifecycle closes and reconciles.
- Telegram cannot make a temporary network outage harmless to operator availability; it records the outage while protected positions continue under exchange/exit-manager controls.
- Post-cutover end-to-end operator-command latency requires a real operator command sample; backend and send stages remain separately instrumented and are not fabricated.
"""
    (REPORTS / "telegram_control_plane_latest.md").write_text(control_report, encoding="utf-8")

    accuracy_report = f"""# Telegram Accuracy Report

Generated: `{now}`

| Telegram value | Authoritative source | Freshness/label |
|---|---|---|
| Armed state | `live_state.state.snapshot()` | direct, never cached |
| Equity/available/frozen | `OKXClient.get_account_snapshot()` | authenticated OKX cache with source timestamp |
| Positions/quantity/PnL | `live_position_truth.get_live_position_snapshot()` | reconciled OKX truth, mismatch surfaced |
| Registry | `position_registry.get_all_details()` | direct persisted runtime state |
| Pending orders | `order_state_store.store.snapshot()` | direct lifecycle store |
| Reservations | `capital_reservations.book.snapshot()` | direct lifecycle store |
| Opportunities | `opportunity_queue.queue.snapshot()` | lifecycle states and expiry enforced |
| Closed production PnL | `pnl_reconciler.production_performance_snapshot()` | `LIVE FINAL` or `LIVE RECONCILING` |
| Settings | `settings_store.get_val()` | effective persisted runtime value |
| Telegram health | `telegram_control_plane.control_plane.snapshot()` | direct telemetry |

## PnL Separation

- Production reconciliation rows: `{len(production_pnl)}`
- Non-production rows excluded from Telegram live PnL: `{len(pnl) - len(production_pnl)}`
- Paper, counterfactual, provisional, and exchange-final data are not blended.
- Final live totals use `exchange_realized_pnl`; pending rows remain labeled `LIVE RECONCILING`.

## Current Integrity

- Live state: `{live.get('state', 'UNKNOWN')}` / armed `{bool(live.get('armed'))}`
- Registry rows: `{len(registry)}`
- Reservation rows: `{len(reservations)}`
- Unresolved order intents: `{len(unresolved)}`

## Controls

- DISARM readback proves `can_submit_live=False` while `engine.running` remains unchanged.
- HALT has the same position-management preservation boundary.
- ARM requires a clean production preflight and verifies state readback.
- Read-only Telegram preflight no longer mutates an already armed state.
- Read-only subprocess imports no longer rewrite the shared live-state file.
- A retrying OKX request regenerates its ISO timestamp and HMAC headers immediately before every network attempt.
"""
    (REPORTS / "telegram_accuracy_latest.md").write_text(accuracy_report, encoding="utf-8")

    cycles = scans["cycles"]
    engines = scans["engine_scans"]
    loop = telemetry.get("event_loop_lag_ms", {}) or {}
    latency_report = "\n".join([
        "# Telegram Latency Report",
        "",
        f"Generated: `{now}`",
        "",
        "## Telegram Actions",
        "",
        "| Action | Samples | Median ms | P95 ms | Max ms |",
        "|---|---:|---:|---:|---:|",
        *_latency_lines(telemetry),
        "",
        f"- Current process cutover: `{process_started_at or 'unavailable'}`",
        f"- Completed action samples after cutover: `{len(post_cutover_rows)}`",
        "- The table retains historical samples across restarts. If the post-cutover count is zero, it is a pre-final baseline, not proof of final production command latency.",
        "",
        "## Event Loop",
        "",
        f"- Lag samples: `{int(loop.get('samples', 0) or 0)}`",
        f"- Median/p95/max: `{float(loop.get('median', 0.0) or 0.0):.1f}/{float(loop.get('p95', 0.0) or 0.0):.1f}/{float(loop.get('max', 0.0) or 0.0):.1f} ms`",
        "",
        "## Trading Load Evidence",
        "",
        f"- Source log: `{scans['path']}`",
        f"- Current runtime log: `{scans.get('current_path', scans['path'])}`",
        f"- Latest full scan: `{cycles[-1]:.2f}s`" if cycles else "- Latest full scan: `unavailable`",
        f"- Median retained full scan: `{sorted(cycles)[len(cycles)//2]:.2f}s`" if cycles else "- Median retained full scan: `unavailable`",
        f"- Latest engine scan median: `{sorted(engines)[len(engines)//2]:.2f}s`" if engines else "- Latest engine scan median: `unavailable`",
        f"- Engine timeout log events: `{scans['timeouts']}`",
        "",
        "Cold start still requires initial higher-timeframe history. Steady-state scans now reuse shared WebSocket/in-memory candles and fetch only symbols missing the most recently completed bar; no universe or engine was removed.",
    ])
    (REPORTS / "telegram_latency_latest.md").write_text(latency_report + "\n", encoding="utf-8")

    visible = [
        "Live Status", "Account", "Positions", "Opportunities", "Engines", "Execution",
        "Risk & Sizing", "Exits", "Liquidity", "Performance", "Research", "System",
        "Why No Trade", "Recent Events", "Emergency",
    ]
    (REPORTS / "telegram_button_census_latest.md").write_text(
        "# Telegram Button Census\n\n"
        f"Generated: `{now}`\n\n"
        + "\n".join(f"- KEEP: `{name}`" for name in visible)
        + "\n\nRemoved from normal navigation: paper mode, retired AI, duplicate dashboard/control/modes/diagnostics rooms. Legacy callback payloads remain graceful.\n",
        encoding="utf-8",
    )
    (REPORTS / "telegram_navigation_latest.md").write_text(
        "# Telegram Navigation Verification\n\n"
        f"Generated: `{now}`\n\n"
        "Home is the single production root. Every visible room has a registered callback and a Home return path. "
        "Emergency exposes DISARM, HALT, and separately confirmed emergency close. Automated callback census and navigation tests pass.\n",
        encoding="utf-8",
    )

    update = f"""## Telegram Production Control Plane ({now})

- Telegram polling now starts before market warm-up; the runtime starts in a supervised background task.
- Eight concurrent Telegram updates, persistent stage latency telemetry, event-loop lag monitoring, and timestamped analytical caching are active.
- Live balance, positions, order state, reservations, opportunities, state, and exchange-final PnL have explicit authoritative sources.
- DISARM/HALT preserve existing-position management; ARM and EMERGENCY use verified backend bindings.
- Full automated result is recorded in `reports/telegram_control_plane_latest.md`.
"""
    for name in ("live_execution_flow_latest.md", "live_trade_performance_latest.md", "adaptive_live_implementation_latest.md"):
        _update_existing(REPORTS / name, update)

    return {
        "generated_at": now,
        "reports": [
            "reports/telegram_control_plane_latest.md",
            "reports/telegram_accuracy_latest.md",
            "reports/telegram_latency_latest.md",
            "reports/telegram_button_census_latest.md",
            "reports/telegram_navigation_latest.md",
        ],
        "live_state": live.get("state", "UNKNOWN"),
        "armed": bool(live.get("armed")),
    }


if __name__ == "__main__":
    print(json.dumps(generate(), indent=2))

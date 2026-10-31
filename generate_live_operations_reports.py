"""Generate concise production execution and exchange-final performance reports."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from statistics import median


DATA = Path("data")
REPORTS = Path("reports")


def _load(path: Path, default):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def _f(value, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _write(name: str, lines: list[str]) -> None:
    REPORTS.mkdir(parents=True, exist_ok=True)
    (REPORTS / name).write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")


def generate() -> dict:
    generated = datetime.now(timezone.utc).isoformat()
    intents = list(_load(DATA / "order_intents.json", {"intents": {}}).get("intents", {}).values())
    flow_events = []
    try:
        flow_events = [json.loads(line) for line in (DATA / "live_execution_events.jsonl").read_text(encoding="utf-8").splitlines() if line.strip()]
    except Exception:
        pass
    pnl_rows = list(_load(DATA / "pnl_reconciliation.json", {"records": {}}).get("records", {}).values())
    live_pnl = [row for row in pnl_rows if str(row.get("execution_environment", "")).lower() in {"live", "production"}]
    final = [row for row in live_pnl if row.get("exchange_final") is True]
    fills = [fill for row in intents for fill in list(row.get("fills", []) or [])]
    states = {state: sum(str(row.get("state", "")) == state for row in intents) for state in {
        "SUBMITTED", "ACKNOWLEDGED", "PARTIALLY_FILLED", "FILLED", "CANCELLED", "REJECTED",
        "ROUTER_VALIDATING", "PAYLOAD_READY", "SUBMISSION_ATTEMPTED", "SUBMIT_UNCERTAIN", "FAILED_FINAL",
    }}
    acknowledged = [row for row in intents if row.get("exchange_ack_at")]
    protected = [row for row in intents if bool(dict(row.get("protection", {}) or {}).get("protected"))]
    fill_latencies = [
        _f(row.get("first_fill_at")) - _f(row.get("router_at"))
        for row in intents if _f(row.get("first_fill_at")) > 0 and _f(row.get("router_at")) > 0
    ]
    slippages = []
    for row in intents:
        arrival = _f(row.get("price"))
        fill = _f(row.get("average_fill_price"))
        if arrival > 0 and fill > 0:
            direction = str(row.get("direction", "")).lower()
            signed = (fill - arrival) / arrival * 100.0
            slippages.append(signed if direction in {"long", "buy"} else -signed)
    pnl_values = [_f(row.get("net_realized_pnl", row.get("exchange_realized_pnl"))) for row in final]
    wins = [value for value in pnl_values if value > 0]
    losses = [value for value in pnl_values if value < 0]
    gross_profit = sum(wins)
    gross_loss = abs(sum(losses))
    profit_factor = gross_profit / gross_loss if gross_loss > 0 else None

    _write("live_execution_flow_latest.md", [
        "# Live Execution Flow", "", f"Generated: {generated}", "",
        "Authoritative path: candidate -> admission -> atomic reservation -> central router -> OKX acknowledgement -> fill -> protection -> exit -> exchange-final reconciliation.", "",
        f"Intents: `{len(intents)}`; acknowledgements: `{len(acknowledged)}`; fills: `{len(fills)}`; protected intents: `{len(protected)}`; durable flow events: `{len(flow_events)}`.", "",
        "## State Counts", "",
        *[f"- `{key}`: {value}" for key, value in sorted(states.items())], "",
        "Lifecycle timestamps are persisted on each intent as signal, candidate, reservation, router, exchange acknowledgement, first/last fill, and protection acknowledgement when available.",
        "A scan hands an admitted trade to an independent execution supervisor. Engine timeout cleanup preserves supervised coins and cannot cancel the worker.",
    ])
    stage_counts = {}
    for row in flow_events:
        stage = str(row.get("stage", "UNKNOWN"))
        stage_counts[stage] = stage_counts.get(stage, 0) + 1
    latest_funnel = _load(DATA / "production_research_funnel_latest.json", {})
    funnel_counts = dict(latest_funnel.get("counts", {}) or {})
    normalized = dict(_load(DATA / "execution_funnel_counters.json", {}).get("counters", {}) or {})
    payload_audit = []
    try:
        payload_audit = [
            json.loads(line)
            for line in (DATA / "payload_terminal_audit.jsonl").read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
    except Exception:
        payload_audit = []
    _write("live_execution_funnel_latest.md", [
        "# Live Execution Funnel", "", f"Generated: {generated}", "",
        f"Symbols seen: `{int(latest_funnel.get('symbols_seen', 0) or 0)}`; data ready: `{int(latest_funnel.get('symbols_data_ready', 0) or 0)}`.",
        f"Raw signals: `{int(funnel_counts.get('raw_signal', 0) or 0)}`; pre-candidates: `{int(funnel_counts.get('pre_candidate', 0) or 0)}`; candidates: `{int(funnel_counts.get('candidate', 0) or 0)}`.",
        "",
        "## Normalized final pre-submit counters",
        "",
        f"- router_requested: `{int(normalized.get('router_requested', stage_counts.get('ROUTER_REQUESTED', 0)) or 0)}`",
        f"- router_validated: `{int(normalized.get('router_validated', stage_counts.get('ROUTER_VALIDATED', 0)) or 0)}`",
        f"- payload_ready: `{int(normalized.get('payload_ready', stage_counts.get('PAYLOAD_READY', 0)) or 0)}`",
        f"- rejected_zero_contracts: `{int(normalized.get('rejected_zero_contracts', 0) or 0)}`",
        f"- rejected_min_notional: `{int(normalized.get('rejected_min_notional', 0) or 0)}`",
        f"- rejected_candidate_stale: `{int(normalized.get('rejected_candidate_stale', 0) or 0)}`",
        f"- rejected_market_stale: `{int(normalized.get('rejected_market_stale', 0) or 0)}`",
        f"- rejected_account_state: `{int(normalized.get('rejected_account_state', 0) or 0)}`",
        f"- rejected_reservation: `{int(normalized.get('rejected_reservation', 0) or 0)}`",
        f"- rejected_leverage: `{int(normalized.get('rejected_leverage', 0) or 0)}`",
        f"- rejected_position_mode: `{int(normalized.get('rejected_position_mode', 0) or 0)}`",
        f"- rejected_economics_after_reprice: `{int(normalized.get('rejected_economics_after_reprice', 0) or 0)}`",
        f"- rejected_other: `{int(normalized.get('rejected_other', 0) or 0)}`",
        f"- submission_attempted: `{int(normalized.get('submission_attempted', stage_counts.get('EXCHANGE_SUBMIT_ATTEMPTED', 0)) or 0)}`",
        f"- exchange_acknowledged: `{int(normalized.get('exchange_acknowledged', stage_counts.get('EXCHANGE_ACKNOWLEDGED', 0)) or 0)}`",
        f"- exchange_rejected: `{int(normalized.get('exchange_rejected', 0) or 0)}`",
        f"- exchange_uncertain: `{int(normalized.get('exchange_uncertain', stage_counts.get('SUBMIT_UNCERTAIN', 0)) or 0)}`",
        f"- filled: `{int(normalized.get('filled', stage_counts.get('FILLED', 0)) or 0)}`",
        "",
        "Local pre-OKX skips are never counted as exchange_rejected.",
        "Strategy rejections remain in the scan funnel; execution infrastructure failures remain in durable execution events.",
    ])
    audit_lines = [
        "# Payload Ready Terminal Audit", "", f"Generated: {generated}", "",
        f"Rows: `{len(payload_audit)}`.",
        "Every PAYLOAD_READY intent should terminate with an explicit class and reason.",
        "",
    ]
    for row in payload_audit[-50:]:
        audit_lines.append(
            f"- `{row.get('intent_id', '')}` `{row.get('coin', '?')}` `{row.get('engine', '?')}` "
            f"→ `{row.get('terminal_class', 'unknown')}` · `{row.get('reason', '')}` · "
            f"exchange_contacted=`{bool(row.get('exchange_contacted'))}`"
        )
    if not payload_audit:
        audit_lines.append("_No payload terminal audit rows yet._")
    _write("payload_terminal_audit_latest.md", audit_lines)
    request_to_ack = []
    by_intent = {}
    for row in flow_events:
        by_intent.setdefault(str(row.get("intent_id", "")), {})[str(row.get("stage", ""))] = _f(row.get("timestamp_epoch"))
    for stages in by_intent.values():
        if stages.get("ROUTER_REQUESTED") and stages.get("EXCHANGE_ACKNOWLEDGED"):
            request_to_ack.append(stages["EXCHANGE_ACKNOWLEDGED"] - stages["ROUTER_REQUESTED"])
    _write("live_execution_latency_latest.md", [
        "# Live Execution Latency", "", f"Generated: {generated}", "",
        f"Router-to-ack samples: `{len(request_to_ack)}`.",
        f"Median router-to-ack: `{median(request_to_ack):.3f}s`." if request_to_ack else "Median router-to-ack: `not available until an exchange acknowledgement`.",
        "Candidate, reservation, router, request, acknowledgement, fill, and protection timestamps are persisted independently.",
    ])
    _write("live_execution_quality_latest.md", [
        "# Live Execution Quality", "", f"Generated: {generated}", "",
        f"Filled intents: `{sum(1 for row in intents if str(row.get('state')) in {'FILLED', 'PARTIALLY_FILLED'})}`.",
        f"Partial-fill intents: `{states.get('PARTIALLY_FILLED', 0)}`.",
        f"Median fill latency: `{median(fill_latencies):.3f}s`." if fill_latencies else "Median fill latency: `not available`.",
        f"Average signed slippage: `{sum(slippages) / len(slippages):+.5f}%`." if slippages else "Average signed slippage: `not available`.",
        f"Median signed slippage: `{median(slippages):+.5f}%`." if slippages else "Median signed slippage: `not available`.",
    ])
    _write("live_exit_capture_latest.md", [
        "# Live Exit Capture", "", f"Generated: {generated}", "",
        f"Exchange-final production closes: `{len(final)}`.",
        "MFE capture, profit surrendered, trail purpose, and final exit attribution remain unavailable until executed positions close with exchange-final evidence." if not final else
        "Exchange-final rows exist; detailed MFE/MAE fields are retained per lifecycle record when supplied by exit management.",
    ])
    _write("exchange_pnl_reconciliation_latest.md", [
        "# Exchange PnL Reconciliation", "", f"Generated: {generated}", "",
        f"Production reconciliation records: `{len(live_pnl)}`; exchange-final: `{len(final)}`.",
        f"Pending/non-final: `{len(live_pnl) - len(final)}`.",
        f"Unexplained exchange-final difference: `${sum(abs(_f(row.get('reconciliation_difference'))) for row in final):.8f}`.",
        "Only exchange-final production records are eligible for production learning.",
    ])
    _write("live_trade_performance_latest.md", [
        "# Live Trade Performance", "", f"Generated: {generated}", "",
        f"Exchange-final trades: `{len(final)}`; wins: `{len(wins)}`; losses: `{len(losses)}`.",
        f"Net PnL: `${sum(pnl_values):+.8f}`.",
        f"Average win: `${sum(wins) / len(wins):+.8f}`." if wins else "Average win: `not available`.",
        f"Average loss: `${sum(losses) / len(losses):+.8f}`." if losses else "Average loss: `not available`.",
        f"Profit factor: `{profit_factor:.4f}`." if profit_factor is not None else "Profit factor: `not available`.",
        "Counterfactual production and historical paper results are intentionally excluded.",
    ])
    return {"intents": len(intents), "fills": len(fills), "exchange_final": len(final)}


if __name__ == "__main__":
    print(json.dumps(generate(), sort_keys=True))

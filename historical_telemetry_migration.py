"""Normalize trustworthy historical paper evidence without inventing identity."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Iterable


def normalize(row: dict[str, Any], *, source_file: str, source_line: int) -> dict[str, Any]:
    stable = hashlib.sha256(f"{source_file}:{source_line}:{json.dumps(row, sort_keys=True)}".encode()).hexdigest()[:24]
    timestamp = row.get("closed_at", row.get("opened_at", row.get("timestamp")))
    is_close = str(row.get("event", "")) == "close" or bool(row.get("closed_at")) or "exit_reason" in row
    return {
        "historical_record_id": stable,
        "source_file": source_file,
        "source_line": source_line,
        "execution_environment": "paper",
        "pnl_provenance": "historical_paper_local" if is_close else "not_applicable",
        "candidate_id": row.get("candidate_id") or None,
        "candidate_id_provenance": "source" if row.get("candidate_id") else "historical_unavailable",
        "lifecycle_id": row.get("lifecycle_id") if row.get("lifecycle_id") and not str(row.get("lifecycle_id")).startswith("PAPER-") else None,
        "lifecycle_id_provenance": "source" if row.get("lifecycle_id") and not str(row.get("lifecycle_id")).startswith("PAPER-") else "historical_unavailable_reused_paper_order_id",
        "engine": row.get("engine") or None,
        "symbol": row.get("pair", row.get("symbol", row.get("coin"))) or None,
        "side": row.get("side", row.get("direction")) or None,
        "timestamp": timestamp or None,
        "opened_at": row.get("opened_at") or None,
        "closed_at": row.get("closed_at", row.get("timestamp") if is_close else None) or None,
        "exit_reason": row.get("exit_reason") or None,
        "blocker_reason": row.get("rejection_reason", row.get("reason")) or None,
        "mfe": row.get("mfe") if row.get("mfe") is not None else None,
        "mae": row.get("mae") if row.get("mae") is not None else None,
        "gross_pnl": row.get("actual_pnl", row.get("pnl_usdt")) if is_close else None,
        "fees": row.get("actual_fee", row.get("fee_usdt")) if is_close else None,
        "net_pnl": row.get("net_pnl", row.get("net_pnl_usdt")) if is_close else None,
        "exchange_final": False,
        "eligible_for_production_learning": False,
    }


def migrate(
    sources: Iterable[Path],
    *,
    output_path: Path = Path("data/historical_telemetry_normalized.jsonl"),
    manifest_path: Path = Path("data/historical_telemetry_manifest.json"),
) -> dict[str, Any]:
    records = []
    counts = {}
    for source in sources:
        if not source.exists():
            continue
        count = 0
        with source.open(encoding="utf-8", errors="replace") as handle:
            for line_no, line in enumerate(handle, 1):
                try:
                    row = json.loads(line)
                except Exception:
                    continue
                if source.name == "novo_trade_memory.jsonl" and row.get("record_type") != "novo_enrichment_outcome":
                    continue
                records.append(normalize(row, source_file=str(source), source_line=line_no))
                count += 1
        counts[str(source)] = count
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("".join(json.dumps(row, sort_keys=True) + "\n" for row in records), encoding="utf-8")
    manifest = {
        "record_count": len(records),
        "sources": counts,
        "execution_environment": "paper",
        "exchange_final": False,
        "eligible_for_production_learning": False,
        "identity_policy": "missing or reused PAPER lifecycle IDs remain null",
    }
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return manifest


if __name__ == "__main__":
    sources = sorted(Path("data").glob("paper_validation_trades*.jsonl")) + [Path("data/novo_trade_memory.jsonl")]
    print(json.dumps(migrate(sources), indent=2))

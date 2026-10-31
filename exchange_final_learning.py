"""Slow, bounded learning fed exclusively by exchange-final reconciliation."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

MODEL_PATH = Path("data/production_learning_model.json")
AUDIT_PATH = Path("data/production_learning_audit.jsonl")
MIN_SAMPLES = 20
MAX_WEIGHT_STEP = 0.02


def eligible(record: dict[str, Any]) -> bool:
    return bool(
        record.get("exchange_final") is True
        and record.get("pnl_status") == "RECONCILED"
        and record.get("exchange_realized_pnl") is not None
        and str(record.get("execution_environment", "production")) == "production"
    )


def _load(path: Path) -> dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {"model_version": 1, "champion": {}, "challenger": {}, "engines": {}}


def ingest(record: dict[str, Any], *, model_path: Path | None = None, audit_path: Path | None = None) -> dict[str, Any]:
    model_path = model_path or MODEL_PATH
    audit_path = audit_path or AUDIT_PATH
    if not eligible(record):
        return {"accepted": False, "reason": "exchange_final_reconciled_production_record_required"}
    model = _load(model_path)
    engine = str(record.get("engine", "UNKNOWN") or "UNKNOWN").upper()
    bucket = dict(model.setdefault("engines", {}).get(engine, {}))
    count = int(bucket.get("sample_count", 0) or 0) + 1
    pnl = float(record.get("exchange_realized_pnl", 0.0) or 0.0)
    prior_mean = float(bucket.get("mean_net_pnl", 0.0) or 0.0)
    mean = prior_mean + (pnl - prior_mean) / count
    bucket.update({"sample_count": count, "mean_net_pnl": round(mean, 8), "last_trade_id": record.get("trade_id")})
    model["engines"][engine] = bucket
    model["updated_at"] = datetime.now(timezone.utc).isoformat()
    model_path.parent.mkdir(parents=True, exist_ok=True)
    model_path.write_text(json.dumps(model, indent=2), encoding="utf-8")
    event = {
        "timestamp": model["updated_at"],
        "trade_id": record.get("trade_id"),
        "engine": engine,
        "accepted": True,
        "model_version": model.get("model_version", 1),
        "sample_count": count,
        "production_weight_update_eligible": count >= MIN_SAMPLES,
        "max_weight_step": MAX_WEIGHT_STEP,
        "rollback_model_version": model.get("model_version", 1),
    }
    with audit_path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(event, sort_keys=True) + "\n")
    return event

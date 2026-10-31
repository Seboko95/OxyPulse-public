"""Append-only advisory memory for Novo intelligence snapshots and outcomes."""
from __future__ import annotations

import json
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


DEFAULT_PATH = Path("data/novo_trade_memory.jsonl")


def _memory_path(path: Any = None) -> Path:
    return Path(path) if path is not None else DEFAULT_PATH


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _numeric(value: Any) -> float | None:
    try:
        number = float(value)
    except Exception:
        return None
    if number != number:
        return None
    return number


def append_memory(record, path=None) -> dict:
    """Append one advisory memory record as JSONL without raising."""
    target = _memory_path(path)
    try:
        if not isinstance(record, dict):
            return {
                "status": "ERROR",
                "path": str(target),
                "written": False,
                "errors": ["record must be a dict"],
            }
        payload = dict(record)
        payload.setdefault("timestamp", _now_iso())
        target.parent.mkdir(parents=True, exist_ok=True)
        with target.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n")
        return {
            "status": "OK",
            "path": str(target),
            "written": True,
            "errors": [],
        }
    except Exception as exc:
        return {
            "status": "ERROR",
            "path": str(target),
            "written": False,
            "errors": [f"{type(exc).__name__}: {exc}"],
        }


def read_recent_memory(limit=100, path=None) -> dict:
    """Read most recent valid memory records, skipping malformed lines safely."""
    target = _memory_path(path)
    errors: list[str] = []
    try:
        limit_int = max(0, int(limit))
    except Exception:
        limit_int = 100
    try:
        if not target.exists():
            return {
                "status": "OK",
                "records": [],
                "count": 0,
                "errors": [],
            }
        records: list[dict] = []
        with target.open("r", encoding="utf-8") as fh:
            for line_no, line in enumerate(fh, start=1):
                text = line.strip()
                if not text:
                    continue
                try:
                    item = json.loads(text)
                except Exception:
                    errors.append(f"malformed json line {line_no}")
                    continue
                if isinstance(item, dict):
                    records.append(item)
                else:
                    errors.append(f"non-object json line {line_no}")
        if limit_int:
            records = records[-limit_int:]
        else:
            records = []
        return {
            "status": "OK",
            "records": records,
            "count": len(records),
            "errors": errors,
        }
    except Exception as exc:
        return {
            "status": "ERROR",
            "records": [],
            "count": 0,
            "errors": [f"{type(exc).__name__}: {exc}"],
        }


def _rank_totals(totals: dict[str, float], reverse: bool) -> list[dict[str, float | str]]:
    ordered = sorted(totals.items(), key=lambda item: item[1], reverse=reverse)
    return [{"name": name, "pnl": round(value, 8)} for name, value in ordered[:5]]


def summarize_memory(limit=500, path=None) -> dict:
    """Summarize recent advisory memory records without raising."""
    try:
        recent = read_recent_memory(limit=limit, path=path)
        records = list(recent.get("records", []) or [])
        errors = list(recent.get("errors", []) or [])
        total_pnl = 0.0
        pnl_seen = False
        wins = 0
        losses = 0
        with_outcome = 0
        quality_scores: list[float] = []
        symbol_pnl: dict[str, float] = defaultdict(float)
        engine_pnl: dict[str, float] = defaultdict(float)
        tier_counts: dict[str, int] = defaultdict(int)

        for record in records:
            outcome = record.get("outcome")
            if outcome not in (None, ""):
                with_outcome += 1
            pnl = _numeric(record.get("pnl"))
            if pnl is not None:
                pnl_seen = True
                total_pnl += pnl
                if pnl > 0:
                    wins += 1
                elif pnl < 0:
                    losses += 1
                symbol = str(record.get("symbol") or "UNKNOWN")
                engine = str(record.get("engine") or "UNKNOWN")
                symbol_pnl[symbol] += pnl
                engine_pnl[engine] += pnl
            quality = _numeric(record.get("quality_score"))
            if quality is not None:
                quality_scores.append(quality)
            tier = str(record.get("quality_tier") or "").strip().upper()
            if tier:
                tier_counts[tier] += 1

        avg_quality = (
            round(sum(quality_scores) / len(quality_scores), 4)
            if quality_scores
            else None
        )
        summary = {
            "total_records": len(records),
            "records_with_outcome": with_outcome,
            "win_count": wins,
            "loss_count": losses,
            "total_pnl": round(total_pnl, 8) if pnl_seen else 0.0,
            "average_quality_score": avg_quality,
            "best_symbols_by_pnl": _rank_totals(symbol_pnl, reverse=True),
            "worst_symbols_by_pnl": _rank_totals(symbol_pnl, reverse=False),
            "best_engines_by_pnl": _rank_totals(engine_pnl, reverse=True),
            "worst_engines_by_pnl": _rank_totals(engine_pnl, reverse=False),
            "quality_tier_counts": dict(sorted(tier_counts.items())),
        }
        return {
            "status": "OK" if recent.get("status") == "OK" else "ERROR",
            "summary": summary,
            "errors": errors,
        }
    except Exception as exc:
        return {
            "status": "ERROR",
            "summary": {
                "total_records": 0,
                "records_with_outcome": 0,
                "win_count": 0,
                "loss_count": 0,
                "total_pnl": 0.0,
                "average_quality_score": None,
                "best_symbols_by_pnl": [],
                "worst_symbols_by_pnl": [],
                "best_engines_by_pnl": [],
                "worst_engines_by_pnl": [],
                "quality_tier_counts": {},
            },
            "errors": [f"{type(exc).__name__}: {exc}"],
        }


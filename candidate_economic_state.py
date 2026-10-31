"""Persist candidate waiting states without suppressing engine scans or ranking."""
from __future__ import annotations

import json
import time
from pathlib import Path
from threading import RLock
from typing import Any


class CandidateEconomicStateStore:
    def __init__(self, path: Path | str = "data/candidate_economic_state.json"):
        self.path = Path(path)
        self._lock = RLock()
        self._rows: dict[str, dict[str, Any]] = {}
        self._load()

    @staticmethod
    def key(engine: str, pair: str, direction: str) -> str:
        return f"{str(engine).upper()}|{str(pair).upper()}|{str(direction).lower()}"

    @staticmethod
    def dependencies(
        *,
        equity: float,
        spread_pct: float,
        signal_score: float,
        risk_capacity_usdt: float,
        execution_floor_policy: str = "account_floor",
    ) -> dict[str, Any]:
        return {
            "equity": round(float(equity or 0.0), 2),
            "spread_pct": round(float(spread_pct or 0.0), 3),
            "signal_score": round(float(signal_score or 0.0), 2),
            "risk_capacity_usdt": round(float(risk_capacity_usdt or 0.0), 2),
            "execution_floor_policy": str(execution_floor_policy or "account_floor"),
        }

    def _load(self) -> None:
        try:
            raw = json.loads(self.path.read_text(encoding="utf-8"))
            self._rows = dict(raw.get("candidates", {}))
        except Exception:
            self._rows = {}

    def _save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps({"candidates": self._rows}, indent=2), encoding="utf-8")

    def unchanged(self, engine: str, pair: str, direction: str, dependencies: dict[str, float]) -> tuple[bool, dict[str, Any]]:
        with self._lock:
            row = dict(self._rows.get(self.key(engine, pair, direction), {}))
            return bool(row and row.get("dependencies") == dependencies), row

    def wait(self, engine: str, pair: str, direction: str, *, state: str, reason: str, dependencies: dict[str, float], evidence: dict | None = None) -> None:
        with self._lock:
            key = self.key(engine, pair, direction)
            prior = self._rows.get(key, {})
            self._rows[key] = {
                "state": str(state),
                "reason": str(reason),
                "dependencies": dict(dependencies),
                "first_seen": float(prior.get("first_seen", time.time())),
                "last_seen": time.time(),
                "observations": int(prior.get("observations", 0)) + 1,
                "evidence": dict(evidence or {}),
            }
            self._save()

    def clear(self, engine: str, pair: str, direction: str) -> None:
        with self._lock:
            if self._rows.pop(self.key(engine, pair, direction), None) is not None:
                self._save()

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return {"count": len(self._rows), "candidates": dict(self._rows)}


store = CandidateEconomicStateStore()

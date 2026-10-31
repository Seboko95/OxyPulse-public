from __future__ import annotations

import time
from dataclasses import dataclass

import settings_store as _s

_signals: dict[str, list[dict]] = {}


@dataclass(frozen=True)
class ConsensusSnapshot:
    coin: str
    direction: str
    engines: list[str]
    agreement_count: int
    avg_confidence: float
    agreement_bonus: float
    last_ts: float

    @property
    def summary(self) -> str:
        if not self.engines:
            return "single-engine"
        return f"{self.agreement_count} aligned: {', '.join(self.engines)}"


def _window_secs() -> float:
    return max(float(_s.get_val("engine_consensus_window_secs", 900.0) or 900.0), 60.0)


def _prune(now: float | None = None) -> None:
    current_ts = float(now or time.time())
    cutoff = current_ts - _window_secs()
    dead: list[str] = []
    for coin, rows in _signals.items():
        keep = [row for row in rows if float(row.get("ts", 0.0) or 0.0) >= cutoff]
        if keep:
            _signals[coin] = keep
        else:
            dead.append(coin)
    for coin in dead:
        _signals.pop(coin, None)


def record_signal(engine: str, coin: str, direction: str, confidence: float) -> None:
    if not bool(_s.get_val("engine_consensus_enabled", True)):
        return
    coin_u = str(coin or "").upper()
    direction_u = str(direction or "").upper()
    if not coin_u or direction_u not in {"LONG", "SHORT"}:
        return
    _prune()
    rows = _signals.setdefault(coin_u, [])
    ts = time.time()
    engine_u = str(engine or "").upper()
    rows = [row for row in rows if not (row.get("engine") == engine_u and row.get("direction") == direction_u)]
    rows.append(
        {
            "engine": engine_u,
            "direction": direction_u,
            "confidence": float(confidence or 0.0),
            "ts": ts,
        }
    )
    _signals[coin_u] = rows


def get_snapshot(coin: str, direction: str) -> ConsensusSnapshot:
    coin_u = str(coin or "").upper()
    direction_u = str(direction or "").upper()
    _prune()
    rows = [
        row for row in _signals.get(coin_u, [])
        if str(row.get("direction", "")).upper() == direction_u
    ]
    if not rows:
        return ConsensusSnapshot(
            coin=coin_u,
            direction=direction_u,
            engines=[],
            agreement_count=0,
            avg_confidence=0.0,
            agreement_bonus=0.0,
            last_ts=0.0,
        )
    engines = sorted({str(row.get("engine", "")).upper() for row in rows if row.get("engine")})
    avg_conf = sum(float(row.get("confidence", 0.0) or 0.0) for row in rows) / max(len(rows), 1)
    bonus_per_engine = float(_s.get_val("engine_consensus_bonus_per_engine", 8.0) or 8.0)
    max_bonus = float(_s.get_val("engine_consensus_max_bonus", 24.0) or 24.0)
    bonus = 0.0
    if len(engines) > 1:
        bonus = min((len(engines) - 1) * bonus_per_engine, max_bonus)
    return ConsensusSnapshot(
        coin=coin_u,
        direction=direction_u,
        engines=engines,
        agreement_count=len(engines),
        avg_confidence=round(avg_conf, 3),
        agreement_bonus=round(bonus, 3),
        last_ts=max(float(row.get("ts", 0.0) or 0.0) for row in rows),
    )


def clear() -> None:
    _signals.clear()

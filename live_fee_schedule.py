"""Authoritative, persisted OKX fee schedule for live economic decisions."""
from __future__ import annotations

import json
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any


DEFAULT_PATH = Path("data/live_fee_schedule.json")


@dataclass(frozen=True)
class FeeSchedule:
    maker: float
    taker: float
    fetched_at: float
    source: str
    inst_type: str = "SWAP"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _fallback() -> FeeSchedule:
    import settings_store as settings

    return FeeSchedule(
        maker=abs(float(settings.get_val("live_maker_fee_rate", 0.0002) or 0.0002)),
        taker=abs(float(settings.get_val("live_taker_fee_rate", 0.0005) or 0.0005)),
        fetched_at=0.0,
        source="configured_fallback",
    )


def load(path: Path | str = DEFAULT_PATH) -> FeeSchedule:
    target = Path(path)
    try:
        raw = json.loads(target.read_text(encoding="utf-8"))
        return FeeSchedule(
            maker=abs(float(raw["maker"])),
            taker=abs(float(raw["taker"])),
            fetched_at=float(raw["fetched_at"]),
            source=str(raw.get("source") or "okx_authenticated"),
            inst_type=str(raw.get("inst_type") or "SWAP"),
        )
    except Exception:
        return _fallback()


def record_okx(
    payload: dict[str, Any],
    *,
    path: Path | str = DEFAULT_PATH,
    fetched_at: float | None = None,
) -> FeeSchedule:
    maker = abs(float(payload.get("maker", payload.get("makerU", 0.0)) or 0.0))
    taker = abs(float(payload.get("taker", payload.get("takerU", 0.0)) or 0.0))
    if maker <= 0.0 and taker <= 0.0:
        raise ValueError("OKX fee schedule contains no nonzero maker/taker rate")
    schedule = FeeSchedule(
        maker=maker,
        taker=taker,
        fetched_at=float(fetched_at or time.time()),
        source="okx_authenticated",
    )
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(schedule.to_dict(), indent=2), encoding="utf-8")
    return schedule


def current(*, execution_type: str = "market", path: Path | str = DEFAULT_PATH) -> tuple[float, FeeSchedule]:
    schedule = load(path)
    rate = schedule.maker if str(execution_type or "").lower() in {"maker", "limit", "post_only"} else schedule.taker
    return abs(float(rate)), schedule


def round_trip_cost(size_usdt: float, *, execution_type: str = "market", path: Path | str = DEFAULT_PATH) -> float:
    rate, _ = current(execution_type=execution_type, path=path)
    return max(float(size_usdt or 0.0), 0.0) * rate * 2.0

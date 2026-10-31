"""Authoritative per-instrument, per-side leverage truth.

This module is deliberately read-only.  It resolves the leverage that OKX
already has configured for an isolated instrument/position side and keeps a
small, freshness-bounded cache so final execution does not issue duplicate
reads for the same truth key.
"""
from __future__ import annotations

import asyncio
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone


class LeverageTruthUnavailable(RuntimeError):
    """Raised when configured leverage cannot be established safely."""


def normalize_side(value: str) -> str:
    value = str(value or "").strip().lower()
    if value in {"buy", "long"}:
        return "long"
    if value in {"sell", "short"}:
        return "short"
    return value


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass(frozen=True)
class LeverageTruth:
    instrument: str
    pos_side: str
    margin_mode: str
    requested_leverage: int
    exchange_configured_leverage: int | None
    position_effective_leverage: int | None = None
    status: str = "UNKNOWN"
    source: str = ""
    timestamp: str = ""
    age_sec: float = 0.0
    mismatch: bool = False
    requested_vs_configured_mismatch: bool = False

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass(frozen=True)
class LeverageFact:
    """Exchange-only leverage fact stored in the freshness cache."""

    instrument: str
    pos_side: str
    margin_mode: str
    exchange_configured_leverage: int
    status: str
    source: str
    timestamp: str
    age_sec: float = 0.0

    def to_dict(self) -> dict:
        return asdict(self)


def _row_side(row: dict) -> str:
    return normalize_side(row.get("posSide", row.get("pos_side", row.get("side", ""))))


def _row_instrument(row: dict) -> str:
    return str(row.get("instId", row.get("instrument", "")) or "").upper()


def select_configured_row(
    rows: list[dict], instrument: str, pos_side: str, margin_mode: str | None = None,
) -> dict | None:
    instrument = str(instrument or "").upper()
    pos_side = normalize_side(pos_side)
    candidates = [row for row in list(rows or []) if isinstance(row, dict)]
    if margin_mode:
        mode = str(margin_mode or "isolated").lower()
        candidates = [row for row in candidates if str(row.get("mgnMode", row.get("margin_mode", mode)) or mode).lower() == mode]
    exact = [row for row in candidates if _row_instrument(row) == instrument and _row_side(row) == pos_side]
    if exact:
        return exact[0]
    # Some OKX responses omit posSide for one-way mode.  Never use a row for
    # another explicit side, but accept an unlabelled row only when it is the
    # sole instrument row.
    unlabelled = [row for row in candidates if _row_instrument(row) == instrument and not _row_side(row)]
    return unlabelled[0] if len(unlabelled) == 1 else None


def configured_from_rows(rows: list[dict], instrument: str, pos_side: str, margin_mode: str | None = None) -> int | None:
    row = select_configured_row(rows, instrument, pos_side, margin_mode)
    if not row:
        return None
    try:
        value = int(float(row.get("lever", row.get("leverage", 0)) or 0))
    except (TypeError, ValueError):
        return None
    return value if value > 0 else None


def position_effective_from_rows(rows: list[dict], instrument: str, pos_side: str) -> int | None:
    instrument = str(instrument or "").upper()
    pos_side = normalize_side(pos_side)
    for row in list(rows or []):
        if not isinstance(row, dict):
            continue
        if _row_instrument(row) != instrument or _row_side(row) != pos_side:
            continue
        try:
            value = int(float(row.get("lever", row.get("leverage", 0)) or 0))
        except (TypeError, ValueError):
            continue
        if value > 0:
            return value
    return None


class LeverageTruthCache:
    """Freshness-bounded and per-key coalesced leverage reads."""

    def __init__(self, *, max_age_sec: float = 30.0):
        self.max_age_sec = max(float(max_age_sec), 0.0)
        self._values: dict[tuple[str, str, str], LeverageFact] = {}
        self._stored_mono: dict[tuple[str, str, str], float] = {}
        self._locks: dict[tuple[str, str, str], asyncio.Lock] = {}
        self._stats = {
            "cache_hits": 0,
            "coalesced_callers": 0,
            "underlying_reads": 0,
        }

    @staticmethod
    def key(instrument: str, pos_side: str, margin_mode: str) -> tuple[str, str, str]:
        return (str(instrument or "").upper(), normalize_side(pos_side), str(margin_mode or "isolated").lower())

    def _lock_for(self, key: tuple[str, str, str]) -> asyncio.Lock:
        return self._locks.setdefault(key, asyncio.Lock())

    def _fresh(self, key: tuple[str, str, str], *, max_age_sec: float | None = None) -> LeverageFact | None:
        value = self._values.get(key)
        stored = self._stored_mono.get(key, 0.0)
        age = time.monotonic() - stored if stored else float("inf")
        limit = self.max_age_sec if max_age_sec is None else max(float(max_age_sec), 0.0)
        if value is None or age > limit:
            return None
        return LeverageFact(**{**value.to_dict(), "age_sec": max(age, 0.0), "source": value.source or "cache"})

    @staticmethod
    def _result(fact: LeverageFact, requested_leverage: int) -> LeverageTruth:
        requested = int(requested_leverage or 0)
        return LeverageTruth(
            instrument=fact.instrument,
            pos_side=fact.pos_side,
            margin_mode=fact.margin_mode,
            requested_leverage=requested,
            exchange_configured_leverage=fact.exchange_configured_leverage,
            status=fact.status,
            source=fact.source,
            timestamp=fact.timestamp,
            age_sec=fact.age_sec,
            mismatch=bool(requested and requested != fact.exchange_configured_leverage),
            requested_vs_configured_mismatch=bool(
                requested and requested != fact.exchange_configured_leverage
            ),
        )

    def stats(self) -> dict:
        return dict(self._stats)

    async def resolve(
        self,
        client,
        instrument: str,
        pos_side: str,
        margin_mode: str = "isolated",
        requested_leverage: int = 1,
        *,
        max_age_sec: float | None = None,
        force: bool = False,
    ) -> LeverageTruth:
        key = self.key(instrument, pos_side, margin_mode)
        if not force:
            cached = self._fresh(key, max_age_sec=max_age_sec)
            if cached is not None:
                self._stats["cache_hits"] += 1
                return self._result(cached, requested_leverage)
        async with self._lock_for(key):
            if not force:
                cached = self._fresh(key, max_age_sec=max_age_sec)
                if cached is not None:
                    self._stats["coalesced_callers"] += 1
                    return self._result(cached, requested_leverage)
            reader = getattr(client, "get_leverage_info", None)
            if not callable(reader):
                raise LeverageTruthUnavailable("leverage_info_reader_missing")
            try:
                rows = await reader(str(instrument).upper(), margin_mode=str(margin_mode or "isolated"))
            except Exception as exc:
                raise LeverageTruthUnavailable(f"leverage_info_read_failed:{type(exc).__name__}:{exc}") from exc
            value = configured_from_rows(
                list(rows or []), str(instrument).upper(), pos_side, margin_mode,
            )
            if value is None:
                raise LeverageTruthUnavailable("leverage_info_side_missing")
            now = utc_now()
            fact = LeverageFact(
                instrument=str(instrument or "").upper(),
                pos_side=normalize_side(pos_side),
                margin_mode=str(margin_mode or "isolated").lower(),
                exchange_configured_leverage=value,
                status="EXCHANGE_CONFIGURED",
                source="okx.account.leverage-info",
                timestamp=now,
                age_sec=0.0,
            )
            self._stats["underlying_reads"] += 1
            self._values[key] = fact
            self._stored_mono[key] = time.monotonic()
            return self._result(fact, requested_leverage)

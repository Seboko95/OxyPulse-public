"""Authoritative OKX futures instrument metadata and conversion helpers."""
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import time
from dataclasses import asdict, dataclass, field
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR
from pathlib import Path
from typing import Any

import settings_store as _s
import latency_telemetry as _latency

logger = logging.getLogger("OxyPulse.InstrumentRegistry")
_PATH = Path("data/instrument_metadata.json")


def _d(value: Any, default: str = "0") -> Decimal:
    try:
        return Decimal(str(value))
    except Exception:
        return Decimal(default)


def _f(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _payload_hash(payload: dict) -> str:
    packed = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(packed.encode("utf-8")).hexdigest()


def _metadata_priority_rank(priority: str) -> int:
    value = str(priority or "").strip().lower()
    if value in {"critical", "execution", "p0"}:
        return 0
    if value in {"p1", "status", "operator"}:
        return 1
    return 2


def _metadata_priority_label(rank: int) -> str:
    return {0: "p0", 1: "p1", 2: "p2"}.get(int(rank), "p2")


@dataclass(frozen=True)
class InstrumentMetadata:
    inst_id: str
    inst_type: str
    settle_ccy: str
    ct_val: float
    ct_val_ccy: str
    ct_type: str
    lot_sz: float
    min_sz: float
    tick_sz: float
    state: str
    max_mkt_sz: float = 0.0
    max_lmt_sz: float = 0.0
    max_leverage: float = 0.0
    refreshed_at: float = 0.0
    source_hash: str = ""
    raw: dict = field(default_factory=dict, compare=False)

    @classmethod
    def from_okx(cls, row: dict, refreshed_at: float | None = None) -> "InstrumentMetadata":
        return cls(
            inst_id=str(row.get("instId", "") or ""),
            inst_type=str(row.get("instType", "") or ""),
            settle_ccy=str(row.get("settleCcy", "") or ""),
            ct_val=_f(row.get("ctVal")),
            ct_val_ccy=str(row.get("ctValCcy", "") or ""),
            ct_type=str(row.get("ctType", "") or ""),
            lot_sz=_f(row.get("lotSz")),
            min_sz=_f(row.get("minSz")),
            tick_sz=_f(row.get("tickSz")),
            state=str(row.get("state", "") or ""),
            max_mkt_sz=_f(row.get("maxMktSz")),
            max_lmt_sz=_f(row.get("maxLmtSz")),
            max_leverage=_f(row.get("lever", row.get("maxLever", 0))),
            refreshed_at=float(refreshed_at or time.time()),
            source_hash=_payload_hash(row),
            raw=dict(row),
        )

    def stale(self, ttl_secs: float | None = None, now: float | None = None) -> bool:
        ttl = float(ttl_secs if ttl_secs is not None else _s.get_val("instrument_metadata_ttl_secs", 3600.0))
        return self.refreshed_at <= 0 or float(now or time.time()) - self.refreshed_at > max(ttl, 1.0)

    def eligible(self) -> tuple[bool, str]:
        if self.state.lower() != "live":
            return False, f"instrument_state_{self.state or 'missing'}"
        if self.inst_type.upper() not in {"SWAP", "FUTURES"}:
            return False, f"unsupported_inst_type_{self.inst_type or 'missing'}"
        if self.ct_val <= 0 or self.lot_sz <= 0 or self.min_sz <= 0 or self.tick_sz <= 0:
            return False, "incomplete_instrument_metadata"
        if self.ct_type.lower() not in {"linear", ""}:
            return False, f"unsupported_contract_type_{self.ct_type}"
        return True, "OK"

    def contract_notional(self, price: float) -> float:
        if self.ct_val <= 0:
            raise ValueError("contract value missing")
        if self.ct_val_ccy.upper() in {"USDT", "USDC", "USD"}:
            return self.ct_val
        if price <= 0:
            raise ValueError("positive price required for base-denominated contract")
        return self.ct_val * float(price)

    def notional_to_raw_contracts(self, notional_usdt: float, price: float) -> float:
        return max(float(notional_usdt), 0.0) / self.contract_notional(price)

    def round_contracts_down(self, raw_contracts: float) -> float:
        lot = _d(self.lot_sz)
        if lot <= 0:
            raise ValueError("lot size missing")
        raw = max(_d(raw_contracts), Decimal("0"))
        return float((raw / lot).to_integral_value(rounding=ROUND_FLOOR) * lot)

    def minimum_executable_contracts(self, notional_floor_usdt: float, price: float) -> float:
        """Return the smallest lot-aligned quantity meeting a floor.

        Ceiling is used only to reach the exchange-defined minimum. The caller
        must revalidate the resulting notional against every safety limit.
        """
        lot = _d(self.lot_sz)
        if lot <= 0 or self.ct_val <= 0 or float(price or 0.0) <= 0:
            raise ValueError("incomplete contract metadata")
        raw = max(_d(notional_floor_usdt), Decimal("0")) / _d(self.contract_notional(price))
        minimum_lots = (_d(self.min_sz) / lot).to_integral_value(rounding=ROUND_CEILING)
        required_lots = (raw / lot).to_integral_value(rounding=ROUND_CEILING)
        lots = max(minimum_lots, required_lots, Decimal("1"))
        return float(lots * lot)

    def notional_to_contracts(self, notional_usdt: float, price: float) -> float:
        return self.round_contracts_down(self.notional_to_raw_contracts(notional_usdt, price))

    def contracts_to_notional(self, contracts: float, price: float) -> float:
        return max(float(contracts), 0.0) * self.contract_notional(price)

    def required_margin(self, contracts: float, price: float, leverage: int) -> float:
        return self.contracts_to_notional(contracts, price) / max(int(leverage), 1)

    def quantity_feasible(self, contracts: float, order_type: str = "market") -> tuple[bool, str]:
        qty = _d(contracts)
        lot = _d(self.lot_sz)
        if qty < _d(self.min_sz):
            return False, "below_exchange_min_size"
        if lot <= 0 or qty % lot != 0:
            return False, "invalid_lot_increment"
        maximum = self.max_mkt_sz if order_type == "market" else self.max_lmt_sz
        if maximum > 0 and float(qty) > maximum:
            return False, "above_exchange_max_size"
        return True, "OK"

    def partial_close_feasible(self, contracts: float) -> bool:
        return self.quantity_feasible(self.round_contracts_down(contracts))[0]

    def runner_feasible(self, total_contracts: float, close_contracts: float) -> bool:
        remaining = self.round_contracts_down(max(float(total_contracts) - float(close_contracts), 0.0))
        return self.quantity_feasible(remaining)[0]

    def round_price(self, raw_price: float, *, side: str, purpose: str) -> float:
        """Round prices without weakening protection or fee-safe profit.

        Long SL rounds up and short SL rounds down so protection cannot move farther
        away. Long TP rounds up and short TP rounds down so profit is not rounded
        toward entry. Passive entries round buy down and sell up.
        """
        tick = _d(self.tick_sz)
        if tick <= 0:
            raise ValueError("tick size missing")
        px = _d(raw_price)
        side_norm = side.lower()
        purpose_norm = purpose.lower()
        round_up = (purpose_norm in {"stop", "sl"} and side_norm == "long") or (
            purpose_norm in {"take_profit", "tp"} and side_norm == "long"
        ) or (purpose_norm in {"entry", "limit"} and side_norm in {"sell", "short"})
        units = (px / tick).to_integral_value(rounding=ROUND_CEILING if round_up else ROUND_FLOOR)
        return float(units * tick)


class InstrumentRegistry:
    def __init__(self, path: Path | str = _PATH):
        self.path = Path(path)
        self._items: dict[str, InstrumentMetadata] = {}
        self._lock = asyncio.Lock()
        self._last_refresh = 0.0
        self._refresh_task: asyncio.Task | None = None
        self._refresh_inflight: dict[tuple[int, int], asyncio.Task] = {}
        self._refresh_sequence = 0
        self._committed_sequence = 0
        self._load()

    @staticmethod
    def environment() -> str:
        return "demo" if os.getenv("OKX_DEMO_TRADING", "false").strip().lower() in {"1", "true", "yes", "on"} else "production"

    def _load(self) -> None:
        try:
            if not self.path.exists():
                return
            raw = json.loads(self.path.read_text(encoding="utf-8"))
            self._last_refresh = _f(raw.get("last_refresh"))
            for row in raw.get("instruments", []):
                item = InstrumentMetadata(**row)
                self._items[item.inst_id] = item
        except Exception as exc:
            logger.warning("Instrument metadata cache load failed: %s", exc)

    def _save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "environment": self.environment(),
            "last_refresh": self._last_refresh,
            "instruments": [asdict(item) for item in self._items.values()],
        }
        self.path.write_text(json.dumps(payload, indent=2, ensure_ascii=True), encoding="utf-8")

    def apply_ws_update(self, row: dict) -> bool:
        """Apply a public instruments-channel update to the canonical memory store.

        The REST snapshot remains the durable authority. WS updates are applied
        in memory only and are persisted by the next governed REST refresh so a
        malformed or transient socket message cannot corrupt the metadata file.
        """
        inst_id = str(row.get("instId", "") or "")
        if not inst_id or str(row.get("settleCcy", "USDT") or "USDT").upper() != "USDT":
            return False
        try:
            item = InstrumentMetadata.from_okx(row, time.time())
        except Exception:
            return False
        self._items[inst_id] = item
        return True

    async def _refresh_impl(self, client, *, priority: str, sequence: int) -> int:
        try:
            response = await client._request(
                "GET", "/api/v5/public/instruments",
                params={"instType": "SWAP"}, priority=priority,
            )
        except TypeError:
            # Lightweight test clients and older adapters may not expose
            # governor priority yet; the production client does.
            response = await client._request(
                "GET", "/api/v5/public/instruments", params={"instType": "SWAP"},
            )
        if str(response.get("code", "0")) != "0":
            raise RuntimeError(f"OKX instrument refresh failed: code={response.get('code')} msg={response.get('msg')}")
        now = time.time()
        items = {
            str(row.get("instId", "")): InstrumentMetadata.from_okx(row, now)
            for row in response.get("data", [])
            if str(row.get("settleCcy", "")).upper() == "USDT" and row.get("instId")
        }
        if not items:
            raise RuntimeError("OKX instrument refresh returned no USDT swaps")
        async with self._lock:
            if sequence >= self._committed_sequence:
                self._items = items
                self._last_refresh = now
                self._committed_sequence = sequence
                self._save()
            count = len(self._items)
        logger.info("Instrument metadata refreshed: %d USDT swaps", len(items))
        return count

    async def refresh(self, client, *, force: bool = False, priority: str = "background") -> int:
        _latency.record_shared_resource("metadata", logical=1)
        ttl = float(_s.get_val("instrument_metadata_ttl_secs", 3600.0))
        if not force and self._items and time.time() - self._last_refresh < ttl:
            _latency.record_shared_resource("metadata", cache_hits=1)
            return len(self._items)
        loop = asyncio.get_running_loop()
        rank = _metadata_priority_rank(priority)
        key = None
        task = None
        # Weaker callers may join stronger metadata work.  Strong callers
        # never inherit a weaker refresh already in progress.
        for candidate_rank in range(rank + 1):
            candidate_key = (id(loop), candidate_rank)
            candidate_task = self._refresh_inflight.get(candidate_key)
            if candidate_task is not None and not candidate_task.done():
                key = candidate_key
                task = candidate_task
                break
        if key is None:
            key = (id(loop), rank)
            task = self._refresh_inflight.get(key)
        if task is None or task.done():
            self._refresh_sequence += 1
            sequence = self._refresh_sequence
            task = asyncio.create_task(
                self._refresh_impl(
                    client,
                    priority=_metadata_priority_label(rank),
                    sequence=sequence,
                ),
                name="instrument-metadata-refresh",
            )
            self._refresh_inflight[key] = task
            _latency.record_shared_resource("metadata", underlying=1)
            def _cleanup(done_task, *, _key=key):
                if self._refresh_inflight.get(_key) is done_task:
                    self._refresh_inflight.pop(_key, None)
            task.add_done_callback(_cleanup)
        else:
            _latency.record_shared_resource("metadata", coalesced=1)
        try:
            return int(await asyncio.shield(task))
        except BaseException:
            _latency.record_shared_resource("metadata", failures=1)
            raise

    async def start_periodic_refresh(self, client) -> None:
        if self._refresh_task and not self._refresh_task.done():
            return
        async def _worker() -> None:
            while True:
                try:
                    await self.refresh(client, force=True)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    logger.warning("Periodic instrument metadata refresh failed: %s", exc)
                await asyncio.sleep(max(float(_s.get_val("instrument_metadata_refresh_secs", 1800.0)), 30.0))
        self._refresh_task = asyncio.create_task(_worker())

    async def stop_periodic_refresh(self) -> None:
        if self._refresh_task and not self._refresh_task.done():
            self._refresh_task.cancel()
            try:
                await self._refresh_task
            except asyncio.CancelledError:
                pass
        self._refresh_task = None

    async def refresh_after_metadata_error(self, client) -> int:
        return await self.refresh(client, force=True)

    def get(self, coin_or_inst: str, *, require_fresh: bool = True) -> InstrumentMetadata | None:
        key = str(coin_or_inst or "").upper()
        if "-" not in key:
            key = f"{key}-USDT-SWAP"
        item = self._items.get(key)
        if item is None or (require_fresh and item.stale()):
            return None
        return item

    def require(self, coin_or_inst: str) -> InstrumentMetadata:
        item = self.get(coin_or_inst, require_fresh=True)
        if item is None:
            raise LookupError(f"missing_or_stale_instrument_metadata:{str(coin_or_inst).upper()}")
        ok, reason = item.eligible()
        if not ok:
            raise ValueError(reason)
        return item

    def snapshot(self) -> dict:
        fresh = sum(1 for item in self._items.values() if not item.stale())
        return {
            "environment": self.environment(),
            "count": len(self._items),
            "fresh_count": fresh,
            "last_refresh": self._last_refresh,
            "healthy": bool(self._items) and fresh == len(self._items),
            "refresh_inflight": bool(self._refresh_inflight),
        }


registry = InstrumentRegistry()

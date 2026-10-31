"""Event-driven funding scheduler for queued, still-valid opportunities."""
from __future__ import annotations

import asyncio
import logging
import time

import micro_sizing
import paper_validation
import settings_store as _s
from capital_reservations import book as reservation_book
from execution_router import ExecutionRequest, new_intent_id, router_for
from instrument_registry import registry as instrument_registry
from opportunity_queue import Opportunity, queue as opportunity_queue
from position_registry import (
    can_open_and_reserve,
    release_pre_submit_reservation as registry_release_pre_submit,
    update as registry_update,
)

logger = logging.getLogger("OxyPulse.CapitalScheduler")


class CapitalScheduler:
    def __init__(self):
        self.client = None
        self.engines: dict = {}
        self._task: asyncio.Task | None = None
        self._event = asyncio.Event()

    def configure(self, client, engines: dict | None = None) -> None:
        self.client = client
        self.engines = dict(engines or {})

    async def start(self) -> None:
        if self._task and not self._task.done():
            return
        self._task = asyncio.create_task(self._loop(), name="capital_scheduler")

    async def stop(self) -> None:
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None

    def notify(self, reason: str = "capacity_changed") -> None:
        logger.debug("Capital scheduler notified: %s", reason)
        self._event.set()

    async def _loop(self) -> None:
        while True:
            try:
                await asyncio.wait_for(self._event.wait(), timeout=max(float(_s.get_val("opportunity_queue_retry_secs", 10.0)), 1.0))
            except asyncio.TimeoutError:
                pass
            self._event.clear()
            try:
                await self.run_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Queued opportunity retry cycle failed")

    async def run_once(self) -> int:
        if self.client is None or not bool(_s.get_val("opportunity_queue_enabled", True)):
            return 0
        now = time.time()
        rows = opportunity_queue.reattemptable(
            now=now,
            rescore=lambda row: row.original_score * max((row.expiry_timestamp - now) / max(row.expiry_timestamp - row.signal_timestamp, 1.0), 0.0),
        )
        funded = 0
        for row in rows:
            if await self._attempt(row):
                funded += 1
        return funded

    async def _attempt(self, opportunity: Opportunity) -> bool:
        payload = dict(opportunity.payload or {})
        try:
            instrument = instrument_registry.require(opportunity.pair)
            ticker = await self.client.get_ticker(opportunity.pair)
            latest = float(ticker.get("last", 0.0) or 0.0)
            entry = float(payload.get("entry_price", opportunity.intended_entry) or opportunity.intended_entry)
            stop = float(payload.get("stop_price", opportunity.stop) or opportunity.stop)
            max_reprice = float(_s.get_val("opportunity_reprice_max_pct", 0.50) or 0.50)
            if latest <= 0 or entry <= 0 or abs(latest - entry) / entry * 100.0 > max_reprice:
                opportunity_queue.remove(opportunity.candidate_id, "stale_market_geometry")
                return False
            if (opportunity.direction == "long" and stop >= latest) or (opportunity.direction == "short" and stop <= latest):
                opportunity_queue.remove(opportunity.candidate_id, "invalidated_stop_geometry")
                return False
            contracts = float(payload.get("approved_contracts", 0.0) or 0.0)
            leverage = max(int(payload.get("leverage", 1) or 1), 1)
            notional = instrument.contracts_to_notional(contracts, latest)
            margin = instrument.required_margin(contracts, latest, leverage)
            snapshot = await self.client.get_account_snapshot()
            balance = float(snapshot.get("equity_usdt", snapshot.get("avail_eq_usdt", 0.0)) or 0.0)
            usable = float(micro_sizing.usable_margin_usdt(balance, _s) or 0.0)
            intent_id = str(payload.get("intent_id") or new_intent_id(opportunity.candidate_id, opportunity.engine, opportunity.pair, opportunity.direction))
            allowed, reason = await can_open_and_reserve(
                opportunity.pair,
                opportunity.direction,
                opportunity.engine,
                notional,
                entry=latest,
                stop_loss=stop,
                metadata={"pending_reservation": True, "queued_candidate_id": opportunity.candidate_id},
                balance_usdt=balance,
            )
            if not allowed:
                if "already" in reason.lower() or "opposing" in reason.lower():
                    opportunity_queue.remove(opportunity.candidate_id, reason)
                return False
            reserved, reserve_reason, reservation_id = await reservation_book.reserve(
                intent_id=intent_id,
                candidate_id=opportunity.candidate_id,
                margin_usdt=margin,
                risk_usdt=float(opportunity.risk_requirement),
                max_total_margin_usdt=usable,
                metadata={"queued": True, "pair": opportunity.pair},
            )
            if not reserved:
                # Capital booking failed before an entry request existed.  This
                # is the only scheduler-owned registry cleanup: it has the
                # explicit pre-submit proof required by the registry contract.
                registry_release_pre_submit(opportunity.pair)
                return False
            request = ExecutionRequest(
                intent_id=intent_id,
                candidate_id=opportunity.candidate_id,
                reservation_id=reservation_id,
                mode=opportunity.deployment_mode,
                engine=opportunity.engine,
                coin=opportunity.pair,
                direction=opportunity.direction,
                requested_contracts=contracts,
                approved_contracts=contracts,
                approved_notional_usdt=notional,
                order_type=str(payload.get("order_type", "market") or "market"),
                price=latest,
                stop_price=instrument.round_price(stop, side=opportunity.direction, purpose="stop"),
                take_profit=instrument.round_price(float(payload.get("exchange_take_profit", 0.0)), side=opportunity.direction, purpose="take_profit") if float(payload.get("exchange_take_profit", 0.0) or 0.0) > 0 else 0.0,
                leverage=leverage,
                metadata={"queued": True},
            )
            result = await router_for(self.client).submit(request)
            if not result.ok:
                # Router failures include post-boundary ambiguity, live
                # partial fills, and protection/recovery work.  None proves
                # exchange position zero, so durable router recovery retains
                # the registry, reservation, and risk-slot ownership.
                return False
            fill = float(result.average_fill_price or latest)
            registry_update(opportunity.pair, metadata={"queued_candidate_id": opportunity.candidate_id, "queue_funded_at": time.time()})
            engine = self.engines.get(opportunity.engine)
            if engine is not None:
                side = "buy" if opportunity.direction == "long" else "sell"
                engine._open[opportunity.pair] = {
                    "side": side,
                    "entry": fill,
                    "size": notional,
                    "leverage": leverage,
                    "stop_loss": stop,
                    "take_profit": float(payload.get("tp1_price", 0.0) or 0.0),
                    "tp_owner": str(payload.get("tp_owner", "") or ""),
                    "tp_mode_at_entry": str(payload.get("tp_mode_at_entry", payload.get("tp_mode", "")) or ""),
                    "order_id": result.order_id,
                    "opened_at": time.time(),
                    "queued_candidate_id": opportunity.candidate_id,
                }
            import exit_manager
            from tp_ownership import UNRESOLVED
            exit_manager.register(
                coin=opportunity.pair,
                direction=opportunity.direction,
                entry_price=fill,
                size_usdt=notional,
                atr_val=float(payload.get("atr_value", 0.0) or 0.0),
                hard_sl=stop,
                tp1_price=float(payload.get("tp1_price", 0.0) or 0.0),
                tp2_price=float(payload.get("tp2_price", 0.0) or 0.0),
                order_id=result.order_id,
                engine_name=opportunity.engine,
                leverage=leverage,
                expected_risk_usdt=float(opportunity.risk_requirement),
                paper_validation={"candidate_id": opportunity.candidate_id, "queued": True},
                tp_owner=str(payload.get("tp_owner", "") or UNRESOLVED),
                tp_mode_at_entry=str(payload.get("tp_mode_at_entry", payload.get("tp_mode", "")) or ""),
                exchange_tp_algo_id=str(payload.get("exchange_tp_algo_id", "") or ""),
                local_tp1_enabled=payload.get("local_tp1_enabled"),
                local_tp2_enabled=payload.get("local_tp2_enabled"),
                ownership_timestamp_value=str(payload.get("ownership_timestamp", "") or ""),
                tp_owner_timestamp_value=str(payload.get("tp_owner_timestamp", payload.get("ownership_timestamp", "")) or ""),
                partials_enabled_at_entry=payload.get("partials_enabled_at_entry"),
            )
            paper_validation.record_entry(
                order_id=result.order_id,
                candidate_id=opportunity.candidate_id,
                pair=opportunity.pair,
                engine=opportunity.engine,
                direction=opportunity.direction,
                entry_price=fill,
                stop_price=stop,
                risk_amount_usdt=float(opportunity.risk_requirement),
                final_size_usdt=notional,
                queued=True,
            )
            opportunity_queue.remove(opportunity.candidate_id, "funded")
            return True
        except Exception as exc:
            logger.warning("Queued opportunity %s retry deferred: %s", opportunity.candidate_id, exc)
            return False


scheduler = CapitalScheduler()

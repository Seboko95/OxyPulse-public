"""
OxyPulse V6 / V6.1 — execution_router.py
Volatility-aware smart execution routing.

V6.1 upgrades over V6:
  - Automatic mode selection based on volatility regime (not just setting)
  - MICRO mode: always market (fast fill, don't miss momentum)
  - GROWTH mode: limit if low vol, market if high vol
  - PRO mode: limit + spread-checked; staged for pullbacks
  - Slippage protection: reject if spread > limit offset
  - Spread check before limit order
  - Capital profile integration

Execution modes:
  market  — immediate fill at best price (always safe fallback)
  limit   — post-only at entry ± ATR offset (requires low spread)
  staged  — laddered 40/35/25 split (TODO_V6: needs OKX multi-leg support)

Note: All modes currently resolve to a single market call to OKX.
The plan is logged for future multi-leg implementation.
"""
from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import time
import uuid
from decimal import Decimal, InvalidOperation
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from typing import Any

import settings_store as _ss
import execution_flow_telemetry as _flow
import executable_minimum as _exec_min
import live_position_truth as _position_truth
from okx_client_ids import canonical_entry_client_id
from okx_mutation_results import (
    MUTATION_DEFINITIVE_REJECTION,
    MutationClassification,
    MUTATION_READBACK_REQUIRED,
    MUTATION_SUCCESS,
    classify_mutation_response,
)
from capital_reservations import book as _reservation_book
from instrument_registry import registry as _instrument_registry
from live_state import state as _live_state
from controlled_lifecycle import gate as _controlled_lifecycle
from order_state_store import store as _order_store
from protection_manager import ProtectionManager

logger = logging.getLogger("OxyPulse.V6.ExecutionRouter")


class ManualCloseSubmissionBoundaryError(RuntimeError):
    """Durable submission-boundary persistence failed before transport."""


_MODE_MARKET = "market"
_MODE_LIMIT  = "limit"
_MODE_STAGED = "staged"

_HIGH_VOL_ATR_PCT = 0.03   # ATR/price > 3% = high volatility
_LOW_VOL_ATR_PCT  = 0.01   # ATR/price < 1% = low volatility

FILL_KNOWN_ZERO = "KNOWN_ZERO"
FILL_KNOWN_POSITIVE = "KNOWN_POSITIVE"
FILL_UNKNOWN = "UNKNOWN_OR_INVALID"
_TERMINAL_CLEANUP_RELEASE_CONFIRMED = "reservation_release_confirmed"


def _authoritative_cumulative_fill(snapshot: dict | None) -> tuple[str, float | None]:
    """Parse only an authoritative cumulative order-fill quantity.

    ``fillSz`` is a single fill event, not cumulative evidence.  It must not
    turn an absent/malformed ``accFillSz`` into a zero-fill cancellation.
    Independently reconciled fill aggregates remain handled by their explicit
    recovery path instead of this snapshot parser.
    """
    snapshot = dict(snapshot or {})
    if "accFillSz" not in snapshot:
        return FILL_UNKNOWN, None
    raw = snapshot.get("accFillSz")
    if raw is None or isinstance(raw, bool) or (isinstance(raw, str) and not raw.strip()):
        return FILL_UNKNOWN, None
    try:
        value = Decimal(str(raw).strip())
    except (InvalidOperation, ValueError):
        return FILL_UNKNOWN, None
    if not value.is_finite() or value < 0:
        return FILL_UNKNOWN, None
    quantity = float(value)
    return (FILL_KNOWN_ZERO if value == 0 else FILL_KNOWN_POSITIVE), quantity


@dataclass
class ExecutionPlan:
    """Describes how an order should be executed."""
    mode: str
    chunks: list[dict] = field(default_factory=list)
    fallback_mode: str = _MODE_MARKET
    notes: str = ""
    spread_ok: bool = True
    economically_viable: bool = True
    blocked_reason: str = ""
    expected_slippage_pct: float = 0.0
    spread_pct: float = 0.0
    liquidity_ratio: float = 0.0


def _market_plan(notes: str = "") -> ExecutionPlan:
    return ExecutionPlan(
        mode=_MODE_MARKET,
        chunks=[{"fraction": 1.0, "price_offset_pct": 0.0}],
        fallback_mode=_MODE_MARKET,
        notes=notes or "market order — immediate fill",
    )


def _limit_plan(
    side: str,
    atr_val: float,
    entry_px: float,
    spread_pct: float = 0.0,
) -> ExecutionPlan:
    if atr_val <= 0 or entry_px <= 0:
        return _market_plan("limit: no ATR data — fallback to market")

    offset_pct = min(atr_val / entry_px * 0.10, 0.002)  # max 0.2% improvement
    sign = -1 if side == "buy" else 1
    price_offset = sign * offset_pct

    # Slippage protection: offset must be larger than half the spread
    if spread_pct > 0 and offset_pct < spread_pct / 2:
        return _market_plan(
            f"limit: spread {spread_pct:.3f}% > 2× offset {offset_pct:.3f}% — market"
        )

    return ExecutionPlan(
        mode=_MODE_LIMIT,
        chunks=[{"fraction": 1.0, "price_offset_pct": price_offset}],
        fallback_mode=_MODE_MARKET,
        notes=f"limit at {offset_pct*100:.3f}% offset (spread={spread_pct:.3f}%)",
        spread_ok=True,
    )


def _staged_plan(
    size_usdt: float,
    atr_val: float,
    entry_px: float,
) -> ExecutionPlan:
    if atr_val <= 0 or entry_px <= 0 or size_usdt <= 0:
        return _market_plan("staged: insufficient data — market")

    step = atr_val / entry_px * 0.05
    chunks = [
        {"fraction": 0.40, "price_offset_pct": 0.0},
        {"fraction": 0.35, "price_offset_pct": -step},
        {"fraction": 0.25, "price_offset_pct": -step * 2},
    ]
    return ExecutionPlan(
        mode=_MODE_STAGED,
        chunks=chunks,
        fallback_mode=_MODE_MARKET,
        notes=f"staged 40/35/25 split step={step*100:.3f}% — TODO_V6: single market until OKX multi-leg",
    )


def _auto_select_mode(
    side: str,
    atr_val: float,
    entry_px: float,
    size_usdt: float,
    spread_pct: float,
    capital_profile,
) -> ExecutionPlan:
    """Automatically choose execution mode based on volatility + capital profile."""
    allow_limit  = bool(_ss.get_val("v6_execution_allow_limit",  False))
    allow_staged = bool(_ss.get_val("v6_execution_allow_staged", False))

    atr_ratio = (atr_val / entry_px) if (atr_val > 0 and entry_px > 0) else 0.0

    mode_name = getattr(capital_profile, "mode", "GROWTH") if capital_profile else "GROWTH"

    # MICRO: always market — don't miss momentum with limit chasing
    if mode_name == "MICRO":
        return _market_plan("MICRO mode: fast market fill")

    # PRO: prefer limit if conditions permit
    if mode_name == "PRO" and allow_limit and atr_ratio <= _LOW_VOL_ATR_PCT:
        return _limit_plan(side, atr_val, entry_px, spread_pct)

    # GROWTH: limit only if low volatility AND spread is tight
    if mode_name == "GROWTH" and allow_limit and atr_ratio <= _LOW_VOL_ATR_PCT:
        if spread_pct < float(_ss.get_val("v6_spread_max_pct", 0.30)) / 2:
            return _limit_plan(side, atr_val, entry_px, spread_pct)

    # High volatility: always market regardless of mode
    if atr_ratio >= _HIGH_VOL_ATR_PCT:
        return _market_plan(f"high vol (ATR={atr_ratio*100:.2f}%) — market")

    return _market_plan(f"default market (mode={mode_name})")


def build_execution_plan(
    coin: str,
    side: str,
    size_usdt: float,
    entry_px: float,
    atr_val: float,
    spread_pct: float = 0.0,
    capital_profile=None,
    liquidity_usdt: float = 0.0,
    urgency: str = "normal",
    signal_half_life_secs: float = 0.0,
    expected_slippage_pct: float = 0.0,
    available_depth_usdt: float = 0.0,
) -> ExecutionPlan:
    """Decide and return the execution plan for a trade candidate.

    Args:
        coin:            Coin ticker (logging only).
        side:            "buy" | "sell".
        size_usdt:       Notional USDT size.
        entry_px:        Current ticker price.
        atr_val:         14-period ATR.
        spread_pct:      Current bid-ask spread as % of price (0 if unavailable).
        capital_profile: CapitalProfile from capital_manager.

    Returns:
        ExecutionPlan — always returns a valid plan (falls back to market on any issue).
    """
    max_spread = float(_ss.get_val("execution_max_economic_spread_pct", _ss.get_val("v6_spread_max_pct", 0.30)) or 0.30)
    max_slippage = float(_ss.get_val("execution_max_slippage_pct", 0.35) or 0.35)
    if spread_pct > max_spread or expected_slippage_pct > max_slippage:
        return ExecutionPlan(
            mode="blocked",
            chunks=[],
            notes="execution economics rejected",
            spread_ok=spread_pct <= max_spread,
            economically_viable=False,
            blocked_reason=(
                f"spread {spread_pct:.4f}% > {max_spread:.4f}%"
                if spread_pct > max_spread
                else f"slippage {expected_slippage_pct:.4f}% > {max_slippage:.4f}%"
            ),
            expected_slippage_pct=expected_slippage_pct,
            spread_pct=spread_pct,
        )
    if not _ss.get_val("v6_smart_execution_enabled", False):
        return _market_plan("v6 execution routing disabled")

    configured_mode = str(_ss.get_val("v6_execution_mode", "market")).lower()

    try:
        depth = max(float(available_depth_usdt or liquidity_usdt or 0.0), 0.0)
        liquidity_ratio = float(size_usdt) / depth if depth > 0 else 0.0
        short_lived = signal_half_life_secs > 0 and signal_half_life_secs <= float(_ss.get_val("execution_short_signal_secs", 30.0) or 30.0)
        if str(urgency).lower() in {"high", "urgent"} or short_lived:
            plan = _market_plan("urgent or short-lived signal")
        elif liquidity_ratio >= float(_ss.get_val("execution_staged_depth_ratio", 0.10) or 0.10) and bool(_ss.get_val("v6_execution_allow_staged", False)):
            plan = _staged_plan(size_usdt, atr_val, entry_px)
        elif configured_mode == "auto":
            plan = _auto_select_mode(side, atr_val, entry_px, size_usdt, spread_pct, capital_profile)
        elif configured_mode == _MODE_LIMIT:
            allow_limit = bool(_ss.get_val("v6_execution_allow_limit", False))
            plan = _limit_plan(side, atr_val, entry_px, spread_pct) if allow_limit else _market_plan("limit disallowed")
        elif configured_mode == _MODE_STAGED:
            allow_staged = bool(_ss.get_val("v6_execution_allow_staged", False))
            plan = _staged_plan(size_usdt, atr_val, entry_px) if allow_staged else _market_plan("staged disallowed")
        else:
            plan = _market_plan(f"configured mode={configured_mode}")
    except Exception as e:
        logger.warning("[V6.Exec] routing error for %s: %s — fallback to market", coin, e)
        plan = _market_plan(f"routing error: {e}")

    logger.debug("[V6.Exec] %s %s → mode=%s: %s", coin, side, plan.mode, plan.notes)
    plan.expected_slippage_pct = float(expected_slippage_pct or 0.0)
    plan.spread_pct = float(spread_pct or 0.0)
    plan.liquidity_ratio = float(size_usdt) / max(float(available_depth_usdt or liquidity_usdt or 0.0), 1e-12) if (available_depth_usdt or liquidity_usdt) else 0.0
    return plan


@dataclass(frozen=True)
class ExecutionRequest:
    intent_id: str
    candidate_id: str
    reservation_id: str
    mode: str
    engine: str
    coin: str
    direction: str
    requested_contracts: float
    approved_contracts: float
    approved_notional_usdt: float
    order_type: str
    price: float = 0.0
    stop_price: float = 0.0
    take_profit: float = 0.0
    reduce_only: bool = False
    margin_mode: str = "isolated"
    leverage: int = 1
    requested_leverage: int | None = None
    exchange_configured_leverage: int | None = None
    position_effective_leverage: int | None = None
    leverage_truth_status: str = ""
    leverage_truth_timestamp: str = ""
    leverage_truth_source: str = ""
    leverage_truth_mismatch: bool = False
    requested_vs_configured_mismatch: bool = False
    configured_vs_effective_mismatch: bool = False
    timeout_secs: float = 15.0
    retry_count: int = 1
    fallback_mode: str = "market"
    metadata: dict = field(default_factory=dict)


@dataclass
class ExecutionResult:
    ok: bool
    reason: str
    intent_id: str
    client_order_id: str
    order_id: str = ""
    state: str = ""
    filled_contracts: float = 0.0
    average_fill_price: float = 0.0
    response: dict = field(default_factory=dict)
    reservation_id: str = ""
    protection: dict = field(default_factory=dict)
    requested_leverage: int | None = None
    exchange_configured_leverage: int | None = None
    position_effective_leverage: int | None = None
    leverage_truth_status: str = ""
    leverage_truth_timestamp: str = ""
    leverage_truth_source: str = ""
    leverage_truth_mismatch: bool = False
    requested_vs_configured_mismatch: bool = False
    configured_vs_effective_mismatch: bool = False

    def legacy_response(self) -> dict | None:
        if not self.ok:
            return None
        row = dict(self.response)
        row.setdefault("ordId", self.order_id)
        row.setdefault("clOrdId", self.client_order_id)
        row.setdefault("fillPx", str(self.average_fill_price or row.get("fillPx", 0)))
        row.setdefault("state", self.state.lower())
        row["execution_intent_id"] = self.intent_id
        row["reservation_id"] = self.reservation_id
        row["filled_contracts"] = self.filled_contracts
        row["protection"] = dict(self.protection)
        row["requested_leverage"] = self.requested_leverage
        row["exchange_configured_leverage"] = self.exchange_configured_leverage
        row["position_effective_leverage"] = self.position_effective_leverage
        row["effective_leverage"] = self.position_effective_leverage
        row["leverage_truth_status"] = self.leverage_truth_status
        row["leverage_truth_timestamp"] = self.leverage_truth_timestamp
        row["leverage_truth_source"] = self.leverage_truth_source
        row["leverage_truth_mismatch"] = self.leverage_truth_mismatch
        row["requested_vs_configured_mismatch"] = self.requested_vs_configured_mismatch
        row["configured_vs_effective_mismatch"] = self.configured_vs_effective_mismatch
        return row


def deterministic_client_order_id(intent_id: str, suffix: str = "E") -> str:
    return canonical_entry_client_id(intent_id, suffix)


class ExecutionRouter:
    """Single idempotent gateway for all OKX order mutations."""

    def __init__(self, client, *, instruments=None, reservations=None, orders=None, live_state=None):
        self.client = client
        self.instruments = instruments or _instrument_registry
        self.reservations = reservations or _reservation_book
        self.orders = orders or _order_store
        self.live_state = live_state or _live_state
        self.protection = ProtectionManager(
            client, instruments=self.instruments, strict_intent_persistence=True,
            emergency_close=self._protection_emergency_close,
        )
        self._locks: dict[str, asyncio.Lock] = {}

    @staticmethod
    def paper_mode() -> bool:
        return os.getenv("PAPER_TRADING", "true").strip().lower() == "true"

    def _lock(self, intent_id: str) -> asyncio.Lock:
        return self._locks.setdefault(intent_id, asyncio.Lock())

    async def _protection_emergency_close(
        self, *, coin: str, pos_side: str, contracts: float,
        source: str, client_order_id: str = "",
    ) -> dict:
        """Route protection failure through the same durable close boundary."""
        seed = str(client_order_id or f"{coin}:{pos_side}:{contracts}")
        digest = hashlib.sha256(seed.encode("utf-8")).hexdigest()[:20].upper()
        return await self.operator_close(
            coin,
            requested_pos_side=pos_side,
            source=source,
            requested_contracts=float(contracts or 0.0),
            intent_id=f"PROTECTION-CLOSE-{str(coin).upper()}-{digest}",
        )

    @staticmethod
    def _release_pre_submit_registry(coin: str) -> None:
        """Release only the router-owned, never-submitted registry row."""
        try:
            from position_registry import release_pre_submit_reservation
            release_pre_submit_reservation(coin)
        except Exception:
            logger.exception("Pre-submit registry cleanup failed for %s", coin)

    async def _pin_reservation_for_router(self, reservation_id: str, *, reason: str) -> bool:
        """Make capacity ownership durable before router work can be interrupted.

        A reservation starts TTL-eligible only while no intent lifecycle has
        claimed it.  Once a router has persisted ``CAPITAL_RESERVED``, a crash
        must retain capacity until explicit terminal accounting releases it.
        Older lightweight test doubles need not implement ``pin``; the real
        durable reservation book does, and a pin failure is fail-closed.
        """
        if not reservation_id:
            return True
        pin = getattr(self.reservations, "pin", None)
        if not callable(pin):
            return True
        try:
            return bool(await pin(
                reservation_id,
                ownership_state="RECOVERY_OWNED",
                reason=reason,
            ))
        except Exception:
            logger.exception("Reservation ownership pin failed for %s", reservation_id)
            return False

    def _pin_reservation_at_boundary(self, reservation_id: str, *, state: str, reason: str) -> bool:
        """Persist exact post-boundary ownership without an await gap before POST."""
        if not reservation_id:
            return True
        pin = getattr(self.reservations, "pin_sync", None)
        if not callable(pin):
            # Router pre-pins production reservations during CAPITAL_RESERVED;
            # injected legacy test doubles have no durable reservation store.
            return True
        try:
            return bool(pin(reservation_id, ownership_state=state, reason=reason))
        except Exception:
            logger.exception("Reservation boundary ownership pin failed for %s", reservation_id)
            return False

    async def _enrich_leverage_truth(self, request: ExecutionRequest) -> ExecutionRequest:
        """Attach read-only configured leverage before final validation."""
        requested = int(request.requested_leverage or request.leverage or 1)
        if self.paper_mode():
            return replace(
                request,
                leverage=requested,
                requested_leverage=requested,
                exchange_configured_leverage=int(request.exchange_configured_leverage or requested),
                leverage_truth_status=request.leverage_truth_status or "PAPER_LOCAL",
                leverage_truth_timestamp=request.leverage_truth_timestamp or "",
                leverage_truth_source=request.leverage_truth_source or "paper",
                leverage_truth_mismatch=bool(request.leverage_truth_mismatch),
                requested_vs_configured_mismatch=bool(request.requested_vs_configured_mismatch),
                configured_vs_effective_mismatch=bool(request.configured_vs_effective_mismatch),
                metadata={
                    **dict(request.metadata or {}),
                    "requested_leverage": requested,
                    "exchange_configured_leverage": int(request.exchange_configured_leverage or requested),
                    "leverage_truth_status": request.leverage_truth_status or "PAPER_LOCAL",
                },
            )
        reader = getattr(self.client, "get_authoritative_leverage", None)
        if not callable(reader):
            # Existing unit-test clients predate the truth API.  Keep those
            # tests deterministic without allowing a production client to
            # silently fall back to strategy leverage.
            if self.client.__class__.__module__ == "okx_client":
                raise RuntimeError("leverage_truth_reader_missing")
            configured = int(request.exchange_configured_leverage or request.leverage or 0)
            return replace(
                request,
                leverage=configured,
                requested_leverage=requested,
                exchange_configured_leverage=configured,
                leverage_truth_status=request.leverage_truth_status or "TEST_STUB",
                leverage_truth_source=request.leverage_truth_source or "test",
                leverage_truth_mismatch=bool(request.leverage_truth_mismatch),
                requested_vs_configured_mismatch=bool(request.requested_vs_configured_mismatch),
                configured_vs_effective_mismatch=bool(request.configured_vs_effective_mismatch),
                metadata={
                    **dict(request.metadata or {}),
                    "requested_leverage": requested,
                    "exchange_configured_leverage": configured,
                    "leverage_truth_status": request.leverage_truth_status or "TEST_STUB",
                },
            )
        try:
            truth = dict(await reader(
                request.coin,
                request.direction,
                requested_leverage=requested,
                margin_mode=request.margin_mode,
            ) or {})
        except Exception as exc:
            raise RuntimeError(f"leverage_truth_unavailable:{type(exc).__name__}:{exc}") from exc
        configured = int(float(truth.get("exchange_configured_leverage", 0) or 0))
        if configured <= 0:
            raise RuntimeError("leverage_truth_unavailable:missing_configured_leverage")
        timestamp = str(truth.get("timestamp", "") or "")
        status = str(truth.get("status", "EXCHANGE_CONFIGURED") or "EXCHANGE_CONFIGURED")
        metadata = {
            **dict(request.metadata or {}),
            "requested_leverage": requested,
            "exchange_configured_leverage": configured,
            "leverage_truth_status": status,
            "leverage_truth_timestamp": timestamp,
            "leverage_truth_source": str(truth.get("source", "") or ""),
            "leverage_truth_mismatch": bool(configured != requested),
            "requested_vs_configured_mismatch": bool(configured != requested),
            "configured_vs_effective_mismatch": False,
        }
        return replace(
            request,
            leverage=configured,
            requested_leverage=requested,
            exchange_configured_leverage=configured,
            leverage_truth_status=status,
            leverage_truth_timestamp=timestamp,
            leverage_truth_source=str(truth.get("source", "") or ""),
            leverage_truth_mismatch=bool(configured != requested),
            requested_vs_configured_mismatch=bool(configured != requested),
            configured_vs_effective_mismatch=False,
            metadata=metadata,
        )

    @staticmethod
    def _effective_position_leverage_from_rows(
        rows: list[dict], instrument: str, direction: str,
    ) -> int | None:
        """Resolve effective leverage from an authoritative position row.

        OKX one-way/net mode reports ``posSide=net`` and uses the signed
        position quantity to identify direction.  A missing or invalid
        leverage field is unknown truth, never a request/local fallback.
        """
        wanted = "long" if str(direction or "").lower() in {"long", "buy"} else "short"
        instrument = str(instrument or "").upper()
        for row in list(rows or []):
            if not isinstance(row, dict):
                continue
            if str(row.get("instId", row.get("instrument", "")) or "").upper() != instrument:
                continue
            try:
                signed_pos = float(row.get("pos", row.get("pos_size", 0.0)) or 0.0)
            except (TypeError, ValueError):
                continue
            if abs(signed_pos) <= 0.0:
                continue
            pos_side = str(row.get("posSide", row.get("side", "")) or "").strip().lower()
            if pos_side in {"long", "buy"}:
                matches = wanted == "long"
            elif pos_side in {"short", "sell"}:
                matches = wanted == "short"
            elif pos_side in {"net", ""}:
                matches = signed_pos > 0 if wanted == "long" else signed_pos < 0
            else:
                matches = False
            if not matches:
                continue
            try:
                effective = int(float(row.get("lever", row.get("leverage", 0)) or 0))
            except (TypeError, ValueError):
                return None
            return effective if effective > 0 else None
        return None

    async def _position_effective_leverage(self, request: ExecutionRequest) -> dict:
        """Read the leverage of the filled position without mutating it."""
        if self.paper_mode():
            return {
                "position_effective_leverage": int(request.exchange_configured_leverage or request.leverage or 1),
                "leverage_truth_status": request.leverage_truth_status or "PAPER_LOCAL",
                "leverage_truth_timestamp": request.leverage_truth_timestamp or "",
                "leverage_truth_source": "paper",
                "leverage_truth_mismatch": False,
                "requested_vs_configured_mismatch": False,
                "configured_vs_effective_mismatch": False,
            }
        reader = getattr(self.client, "get_positions_status", None)
        if not callable(reader):
            return {
                "position_effective_leverage": None,
                "leverage_truth_status": "LEVERAGE_UNKNOWN",
                "leverage_truth_timestamp": "",
                "leverage_truth_source": "position_truth_unavailable",
                "leverage_truth_mismatch": False,
                "requested_vs_configured_mismatch": bool(
                    request.exchange_configured_leverage
                    and request.exchange_configured_leverage != request.requested_leverage
                ),
                "configured_vs_effective_mismatch": False,
            }
        try:
            try:
                status = await reader(force_refresh=True, priority="P0")
            except TypeError:
                status = await reader()
            rows = list(status.get("positions", []) or []) if isinstance(status, dict) else list(status or [])
            wanted = str(request.coin or "").upper()
            wanted = wanted if "-USDT-" in wanted else f"{wanted}-USDT-SWAP"
            match = next((row for row in rows if self._effective_position_leverage_from_rows(
                [row], wanted, request.direction,
            ) is not None), None)
            if not match:
                return {
                    "position_effective_leverage": None,
                    "leverage_truth_status": "LEVERAGE_UNKNOWN",
                    "leverage_truth_timestamp": "",
                    "leverage_truth_source": "position_not_found_after_fill",
                    "leverage_truth_mismatch": False,
                    "requested_vs_configured_mismatch": bool(
                        request.exchange_configured_leverage
                        and request.exchange_configured_leverage != request.requested_leverage
                    ),
                    "configured_vs_effective_mismatch": False,
                }
            effective = self._effective_position_leverage_from_rows([match], wanted, request.direction)
            if effective is None:
                raise ValueError("position_leverage_missing")
            return {
                "position_effective_leverage": effective,
                "exchange_position_evidence": dict(match),
                "leverage_truth_status": "POSITION_EFFECTIVE",
                "leverage_truth_timestamp": str(match.get("uTime", "") or time.time()),
                "leverage_truth_source": "okx.position",
                "leverage_truth_mismatch": bool(effective != int(request.exchange_configured_leverage or request.leverage or effective)),
                "requested_vs_configured_mismatch": bool(
                    request.exchange_configured_leverage
                    and request.exchange_configured_leverage != request.requested_leverage
                ),
                "configured_vs_effective_mismatch": bool(
                    effective != int(request.exchange_configured_leverage or effective)
                ),
            }
        except Exception as exc:
            return {
                "position_effective_leverage": None,
                "leverage_truth_status": "LEVERAGE_UNKNOWN",
                "leverage_truth_timestamp": str(time.time()),
                "leverage_truth_source": f"position_truth_error:{type(exc).__name__}",
                "leverage_truth_mismatch": False,
                "requested_vs_configured_mismatch": bool(
                    request.exchange_configured_leverage
                    and request.exchange_configured_leverage != request.requested_leverage
                ),
                "configured_vs_effective_mismatch": False,
            }

    def _local_executable_check(
        self,
        request: ExecutionRequest,
        *,
        balance_usdt: float | None = None,
    ) -> tuple[bool, str, dict]:
        """Deterministic local checks that must pass before PAYLOAD_READY."""
        meta = dict(request.metadata or {})
        contracts = float(request.approved_contracts or 0.0)
        notional = float(request.approved_notional_usdt or 0.0)
        balance = float(balance_usdt or 0.0)
        if balance <= 0.0:
            balance = float(
                meta.get("allocator_equity")
                or meta.get("balance_usdt")
                or meta.get("equity_usdt")
                or 0.0
            )
        instrument_min = float(
            meta.get("instrument_min_notional_usdt")
            or 0.0
        )
        contract_aware = bool(meta.get("contract_aware_minimum", False))
        if contract_aware and instrument_min <= 0.0:
            try:
                instrument = self.instruments.require(request.coin)
                instrument_min = float(instrument.min_sz) * float(
                    instrument.contract_notional(float(request.price or 0.0))
                )
            except Exception:
                contract_aware = False
        if balance > 0:
            min_notional = _exec_min.placement_min_notional_usdt(
                balance,
                instrument_min_notional_usdt=instrument_min,
                settings=_ss,
                contract_aware=contract_aware,
            )
        else:
            # Without equity context, still enforce known absolute/instrument components
            # without applying the large non-adaptive account base floor.
            min_notional = max(
                instrument_min,
                float(_ss.get_val("adaptive_min_economic_notional_usdt", 1.0) or 1.0),
                1.0,
            )
        detail = {
            "approved_contracts": contracts,
            "approved_notional_usdt": notional,
            "placement_min_notional_usdt": min_notional,
            "balance_usdt": balance,
            "balance_source": "live_revalidation" if balance_usdt is not None else "request_metadata",
            "contract_aware_minimum": contract_aware,
        }
        participation_cap = meta.get("participation_cap_usdt")
        if participation_cap is not None:
            participation_cap = max(float(participation_cap or 0.0), 0.0)
            detail["participation_cap_usdt"] = participation_cap
        if contracts <= 0:
            return False, f"rejected_zero_contracts: approved_contracts={contracts}", {**detail, "funnel_counter": "rejected_zero_contracts"}
        if participation_cap is not None and notional > participation_cap + 1e-9:
            return (
                False,
                f"participation_cap_exceeded: notional=${notional:.4f} cap=${participation_cap:.4f}",
                {**detail, "funnel_counter": "rejected_participation_cap"},
            )
        if not _exec_min.notional_meets_placement_floor(notional, min_notional):
            return (
                False,
                f"rejected_min_notional: notional=${notional:.4f} < placement_floor=${min_notional:.4f}",
                {**detail, "funnel_counter": "rejected_min_notional"},
            )
        return True, "OK", detail

    async def _reject_local(
        self,
        request: ExecutionRequest,
        *,
        reason: str,
        funnel_counter: str = "",
        stage: str = "PRE_OKX_SKIP",
        extra: dict | None = None,
        client_order_id: str = "",
    ) -> ExecutionResult:
        counter = funnel_counter or _exec_min.classify_pre_okx_reason(reason)
        fields = {
            "intent_id": request.intent_id,
            "candidate_id": request.candidate_id,
            "reservation_id": request.reservation_id,
            "engine": request.engine,
            "coin": request.coin,
            "direction": request.direction,
            "reason": reason,
            "funnel_counter": counter,
            "terminal_class": counter,
            "approved_contracts": request.approved_contracts,
            "approved_notional_usdt": request.approved_notional_usdt,
            "exchange_contacted": False,
            **dict(extra or {}),
        }
        if client_order_id:
            fields["client_order_id"] = client_order_id
        if stage.upper() == "PRE_OKX_SKIP":
            _flow.record_pre_okx_skip(**fields)
        else:
            _flow.record(stage, **fields)
        existing = self.orders.get(request.intent_id)
        if not existing:
            self.orders.create({
                **request.__dict__,
                "client_order_id": client_order_id or deterministic_client_order_id(request.intent_id),
                "state": "INTENT_CREATED",
            })
        row = await self._finalize_terminal(
            request,
            state="REJECTED",
            # Keep the established detailed rejection class visible to the
            # execution funnel while also persisting the R4 terminal family.
            terminal_class=counter,
            reason=reason,
            exchange_contacted=False,
            fields={
                "terminal_family": "local_rejection",
                "rejection_reason": reason,
                "funnel_counter": counter,
                **dict(extra or {}),
            },
        )
        self._release_pre_submit_registry(request.coin)
        return self._result_from_row(row, reason, ok=False)

    async def _finalize_terminal(
        self,
        request: ExecutionRequest,
        *,
        state: str,
        terminal_class: str,
        reason: str,
        exchange_contacted: bool,
        fields: dict | None = None,
    ) -> dict:
        """The sole idempotent entry-terminal accounting path.

        ``intent_id`` is the idempotency key.  Durable finalization happens
        before its telemetry row, so a duplicate caller cannot emit a second
        terminal audit record after a retry or restart.
        """
        if terminal_class not in _exec_min.EXECUTION_TERMINAL_CLASSES and terminal_class not in _exec_min.FUNNEL_COUNTER_KEYS:
            raise ValueError(f"unknown execution terminal class: {terminal_class}")
        telemetry_event = {
            "terminal_event_id": f"terminal:{request.intent_id}",
            "intent_id": request.intent_id,
            "terminal_class": terminal_class,
            "reason": reason,
            "exchange_contacted": bool(exchange_contacted),
            "candidate_id": request.candidate_id,
            "reservation_id": request.reservation_id,
            "engine": request.engine,
            "coin": request.coin,
            "direction": request.direction,
            "state": state,
        }
        row, newly_finalized = self.orders.finalize_terminal(
            request.intent_id,
            state,
            terminal_class=terminal_class,
            terminal_reason=reason,
            exchange_contacted=bool(exchange_contacted),
            terminal_telemetry_event=telemetry_event,
            terminal_telemetry_status="pending",
            terminal_telemetry_error="",
            **dict(fields or {}),
        )
        await self._flush_terminal_telemetry(request.intent_id)
        # A staged parent's reservation protects the full staged mutation.
        # Children deliberately inherit its ID for validation, but never own
        # its release.  Cleanup status is durable and separate from immutable
        # terminal truth so a failed release can be retried safely later.
        row = await self._retry_terminal_cleanup(self.orders.get(request.intent_id) or row)
        return self.orders.get(request.intent_id) or row

    async def _flush_terminal_telemetry(self, intent_id: str) -> None:
        """Deliver one durable terminal outbox row; never alter terminal truth."""
        row = self.orders.get(intent_id) or {}
        if row.get("terminal_telemetry_status") == "complete":
            return
        event = dict(row.get("terminal_telemetry_event", {}) or {})
        if not event:
            return
        event["client_order_id"] = str(row.get("client_order_id", "") or "")
        event["order_id"] = str(row.get("exchange_order_id", "") or "")
        try:
            _flow.record_payload_terminal(**event)
        except Exception as exc:
            logger.exception("Terminal telemetry pending for %s", intent_id)
            self.orders.mark_terminal_telemetry(intent_id, status="pending", error=f"{type(exc).__name__}:{exc}")
            return
        try:
            self.orders.mark_terminal_telemetry(intent_id, status="complete", error="")
        except Exception:
            # The sink dedupes terminal_event_id, so this is intentionally
            # left pending for a restart-safe retry.
            logger.exception("Terminal telemetry acknowledgement pending for %s", intent_id)

    async def _retry_terminal_cleanup(self, row: dict) -> dict:
        """Retry only the local, idempotent reservation release for a terminal row."""
        intent_id = str(row.get("intent_id", "") or "")
        reservation_id = str(row.get("reservation_id", "") or "")
        metadata = dict(row.get("metadata", {}) or {})
        owner = str(metadata.get("reservation_owner_intent_id", intent_id) or intent_id)
        if not intent_id or owner != intent_id or not reservation_id:
            return row
        if row.get("terminal_cleanup_status") == "complete":
            return row
        if (
            row.get("terminal_cleanup_status") == "pending"
            and row.get("terminal_cleanup_error") == _TERMINAL_CLEANUP_RELEASE_CONFIRMED
        ):
            # The release was positively confirmed before a later completion
            # acknowledgement failed; never replay that confirmed release.
            return self.orders.mark_terminal_cleanup(intent_id, status="complete", error="")
        self.orders.mark_terminal_cleanup(intent_id, status="pending", error="")
        try:
            released = await self.reservations.release(reservation_id)
        except Exception as exc:
            logger.exception("Reservation cleanup pending for terminal intent %s", intent_id)
            return self.orders.mark_terminal_cleanup(intent_id, status="pending", error=f"{type(exc).__name__}:{exc}")
        if not released:
            # A terminal row is immutable truth, but cleanup is complete only
            # when the reservation store positively proves removal.  Missing
            # or unhealthy state must remain retryable and capacity-owned.
            logger.error("Reservation cleanup not confirmed for terminal intent %s", intent_id)
            return self.orders.mark_terminal_cleanup(
                intent_id, status="pending", error="reservation_release_not_confirmed"
            )
        self.orders.mark_terminal_cleanup(
            intent_id, status="pending", error=_TERMINAL_CLEANUP_RELEASE_CONFIRMED
        )
        return self.orders.mark_terminal_cleanup(intent_id, status="complete", error="")

    @staticmethod
    def _request_from_row(row: dict) -> ExecutionRequest:
        """Rebuild only durable request facts; used by local recovery paths."""
        return ExecutionRequest(
            intent_id=str(row.get("intent_id", "") or ""),
            candidate_id=str(row.get("candidate_id", "") or ""),
            reservation_id=str(row.get("reservation_id", "") or ""),
            mode=str(row.get("mode", "Guarded") or "Guarded"),
            engine=str(row.get("engine", "RECOVERED") or "RECOVERED"),
            coin=str(row.get("coin", "") or ""), direction=str(row.get("direction", "long") or "long"),
            requested_contracts=float(row.get("requested_contracts", 0.0) or 0.0),
            approved_contracts=float(row.get("approved_contracts", 0.0) or 0.0),
            approved_notional_usdt=float(row.get("approved_notional_usdt", 0.0) or 0.0),
            order_type=str(row.get("order_type", "market") or "market"),
            price=float(row.get("price", 0.0) or 0.0), stop_price=float(row.get("stop_price", 0.0) or 0.0),
            take_profit=float(row.get("take_profit", 0.0) or 0.0),
            reduce_only=bool(row.get("reduce_only", False)),
            margin_mode=str(row.get("margin_mode", "isolated") or "isolated"),
            leverage=int(row.get("leverage", 1) or 1),
            requested_leverage=(int(row["requested_leverage"]) if row.get("requested_leverage") not in (None, "") else None),
            exchange_configured_leverage=(int(row["exchange_configured_leverage"]) if row.get("exchange_configured_leverage") not in (None, "") else None),
            position_effective_leverage=(int(row["position_effective_leverage"]) if row.get("position_effective_leverage") not in (None, "") else None),
            leverage_truth_status=str(row.get("leverage_truth_status", "") or ""),
            leverage_truth_timestamp=str(row.get("leverage_truth_timestamp", "") or ""),
            leverage_truth_source=str(row.get("leverage_truth_source", "") or ""),
            leverage_truth_mismatch=bool(row.get("leverage_truth_mismatch", False)),
            requested_vs_configured_mismatch=bool(row.get("requested_vs_configured_mismatch", False)),
            configured_vs_effective_mismatch=bool(row.get("configured_vs_effective_mismatch", False)),
            timeout_secs=float(row.get("timeout_secs", 15.0) or 15.0),
            retry_count=int(row.get("retry_count", 1) or 1),
            fallback_mode=str(row.get("fallback_mode", "market") or "market"),
            metadata=dict(row.get("metadata", {}) or {}),
        )

    @staticmethod
    def _staged_manifest(request: ExecutionRequest, instrument, chunks: list[dict]) -> dict:
        """Build the parent-owned, immutable authorization for staged children."""
        children: list[dict] = []
        allocated = 0.0
        for index, chunk in enumerate(chunks):
            fraction = max(float(chunk.get("fraction", 0.0) or 0.0), 0.0)
            quantity = instrument.round_contracts_down(request.approved_contracts * fraction)
            if index == len(chunks) - 1:
                quantity = max(quantity, instrument.round_contracts_down(request.approved_contracts - allocated))
            if quantity <= 0 or not instrument.quantity_feasible(quantity, "limit")[0]:
                continue
            allocated += quantity
            raw_price = request.price * (1.0 + float(chunk.get("price_offset_pct", 0.0) or 0.0))
            children.append({
                "intent_id": f"{request.intent_id}-S{index + 1}",
                "stage_index": index + 1,
                "requested_contracts": quantity,
                "approved_contracts": quantity,
                "approved_notional_usdt": instrument.contracts_to_notional(quantity, request.price),
                "price": instrument.round_price(raw_price, side=request.direction.lower(), purpose="entry"),
            })
        return {
            "version": 1,
            "staged_parent_intent_id": request.intent_id,
            "reservation_owner_intent_id": request.intent_id,
            "reservation_id": request.reservation_id,
            "stage_count": len(children),
            "children": children,
        }

    @staticmethod
    def _staged_manifest_entry(parent: dict, child_intent_id: str) -> dict | None:
        manifest = dict(parent.get("staged_manifest", {}) or {})
        for entry in list(manifest.get("children", []) or []):
            if str(entry.get("intent_id", "") or "") == child_intent_id:
                return dict(entry)
        return None

    def _staged_child_authorized(self, request: ExecutionRequest, child: dict, parent: dict, reservation: dict) -> bool:
        """Require parent truth, not child naming or self-declared metadata."""
        metadata = dict(child.get("metadata", {}) or {})
        manifest = dict(parent.get("staged_manifest", {}) or {})
        entry = self._staged_manifest_entry(parent, request.intent_id)
        owner = str(request.metadata.get("reservation_owner_intent_id", request.intent_id) or request.intent_id)
        if not entry:
            return False
        try:
            stage_index = int(metadata.get("staged_stage_index", 0) or 0)
            stage_count = int(metadata.get("staged_stage_count", 0) or 0)
        except (TypeError, ValueError):
            return False
        return (
            str(manifest.get("staged_parent_intent_id", "") or "") == parent.get("intent_id")
            and str(manifest.get("reservation_owner_intent_id", "") or "") == parent.get("intent_id")
            and str(manifest.get("reservation_id", "") or "") == request.reservation_id
            and str(metadata.get("staged_parent_intent_id", "") or "") == parent.get("intent_id")
            and str(metadata.get("reservation_owner_intent_id", "") or "") == parent.get("intent_id")
            and owner == parent.get("intent_id")
            and str(parent.get("reservation_id", "") or "") == request.reservation_id
            and str(reservation.get("intent_id", "") or "") == parent.get("intent_id")
            and stage_index == int(entry.get("stage_index", -1) or -1)
            and stage_count == int(manifest.get("stage_count", -1) or -1)
            and float(child.get("requested_contracts", 0.0) or 0.0) == float(entry.get("requested_contracts", -1.0) or -1.0)
            and float(child.get("approved_contracts", 0.0) or 0.0) == float(entry.get("approved_contracts", -1.0) or -1.0)
            and float(child.get("approved_notional_usdt", 0.0) or 0.0) == float(entry.get("approved_notional_usdt", -1.0) or -1.0)
            and float(child.get("price", 0.0) or 0.0) == float(entry.get("price", -1.0) or -1.0)
        )

    def _staged_child_request(self, parent_request: ExecutionRequest, parent: dict, entry: dict) -> ExecutionRequest:
        manifest = dict(parent.get("staged_manifest", {}) or {})
        return ExecutionRequest(**{
            **parent_request.__dict__,
            "intent_id": str(entry["intent_id"]),
            "requested_contracts": float(entry["requested_contracts"]),
            "approved_contracts": float(entry["approved_contracts"]),
            "approved_notional_usdt": float(entry["approved_notional_usdt"]),
            "order_type": "limit",
            "price": float(entry["price"]),
            "metadata": {
                **dict(parent_request.metadata or {}),
                "reservation_owner_intent_id": parent_request.intent_id,
                "staged_parent_intent_id": parent_request.intent_id,
                "staged_stage_index": int(entry["stage_index"]),
                "staged_stage_count": int(manifest["stage_count"]),
            },
        })

    def _mark_recovery_pending(
        self,
        request: ExecutionRequest,
        *,
        state: str,
        reason: str,
        **fields: Any,
    ) -> dict:
        """Durably identify a nonterminal intent that must be read back only."""
        fields = dict(fields)
        if "filled_contracts" not in fields or "average_fill_price" not in fields:
            previous = self.orders.get(request.intent_id) or {}
            if "filled_contracts" not in fields:
                raw_filled = previous.get("filled_contracts", 0.0)
                try:
                    fields["filled_contracts"] = float(raw_filled or 0.0)
                except (TypeError, ValueError):
                    # Invalid prior quantity is not fill evidence; retain the
                    # recovery row with an explicit zero until authoritative
                    # cumulative fill truth is available.
                    fields["filled_contracts"] = 0.0
            if "average_fill_price" not in fields:
                raw_average = previous.get("average_fill_price", 0.0)
                try:
                    fields["average_fill_price"] = float(raw_average or 0.0)
                except (TypeError, ValueError):
                    fields["average_fill_price"] = 0.0
        row = self.orders.transition(
            request.intent_id,
            state,
            recovery_required=True,
            recovery_reason=reason,
            terminal_class="exchange_uncertain" if state in {"SUBMIT_UNCERTAIN", "RECOVERY_QUERY"} else "acknowledged_unfilled",
            **fields,
        )
        # This is descriptive strengthening of an already fail-closed router
        # pin.  A failure cannot free the reservation; it remains
        # RECOVERY_OWNED and the intent remains nonterminal.
        self._pin_reservation_at_boundary(
            request.reservation_id,
            state=state if state in {
                "SUBMISSION_ATTEMPTED", "SUBMITTED", "ACKNOWLEDGED", "SUBMIT_UNCERTAIN",
                "RECOVERY_QUERY", "PARTIALLY_FILLED", "CANCEL_PENDING",
            } else "RECOVERY_OWNED",
            reason=reason,
        )
        _flow.record(
            "RECOVERY_PENDING",
            intent_id=request.intent_id,
            candidate_id=request.candidate_id,
            reservation_id=request.reservation_id,
            engine=request.engine,
            coin=request.coin,
            direction=request.direction,
            reason=reason,
            recovery_state=state,
            funnel_counter="exchange_uncertain" if state in {"SUBMIT_UNCERTAIN", "RECOVERY_QUERY"} else "exchange_acknowledged",
        )
        return row

    async def _fresh_position_truth(self) -> tuple[bool, str, dict]:
        """Require current authoritative exchange exposure before live payloads."""
        try:
            from position_registry import get_all_details

            registry = get_all_details()
            cached = _position_truth.cached_live_position_snapshot()
            settings = _ss
            if _position_truth.execution_position_truth_is_safe(cached, settings):
                return True, "OK", {
                    "position_truth": cached,
                    "position_truth_source": "cached_authoritative",
                }

            # A fresh failed read should not be hammered once per candidate;
            # let its normal refresh TTL elapse before the next direct read.
            if cached is not None and not bool(cached.get("ok")):
                retry_after = max(float(_ss.get_val("live_position_refresh_sec", 3.0) or 3.0), 1.0)
                if float(cached.get("age_sec", 0.0) or 0.0) < retry_after:
                    return False, "position_truth_unavailable", {
                        "position_truth": cached,
                        "funnel_counter": "rejected_account_state",
                    }

            snapshot = await _position_truth.get_live_position_snapshot(
                self.client,
                registry=registry,
                settings=settings,
                force_refresh=True,
            )
            if _position_truth.execution_position_truth_is_safe(snapshot, settings):
                return True, "OK", {
                    "position_truth": snapshot,
                    "position_truth_source": "fresh_authoritative",
                }
            if not isinstance(snapshot, dict) or not bool(snapshot.get("ok")):
                return False, "position_truth_unavailable", {
                    "position_truth": snapshot,
                    "funnel_counter": "rejected_account_state",
                }
            return False, "position_truth_mismatch", {
                "position_truth": snapshot,
                "funnel_counter": "rejected_account_state",
            }
        except Exception as exc:
            return False, "position_truth_unavailable", {
                "position_truth_error": f"{type(exc).__name__}: {exc}",
                "funnel_counter": "rejected_account_state",
            }

    async def _validate_live_submission(self, request: ExecutionRequest) -> tuple[bool, str, dict]:
        """Revalidate mutable account and market state immediately before submission."""
        if self.paper_mode():
            return True, "paper_mode", {}
        armed, reason = self.live_state.can_submit_live()
        if not armed:
            return False, reason, {}
        if str(request.margin_mode or "").lower() != "isolated":
            return False, "isolated_margin_required", {}
        min_leverage = int(float(_ss.get_val("leverage_min", 5) or 5))
        max_leverage = int(float(_ss.get_val("leverage_max", 15) or 15))
        if not min_leverage <= int(request.leverage) <= max_leverage:
            return False, f"leverage_out_of_bounds:{request.leverage}", {}
        try:
            instrument = self.instruments.require(request.coin)
        except Exception as exc:
            return False, str(exc), {}
        reservation = self.reservations.get(request.reservation_id)
        if not reservation:
            return False, "capital_reservation_missing_or_expired", {}
        reservation_owner = str(
            request.metadata.get("reservation_owner_intent_id", request.intent_id)
            or request.intent_id
        )
        if str(reservation.get("intent_id", "")) != request.intent_id:
            # Shared reservations are narrowly valid only for a durable child
            # relationship whose parent owns this exact reservation.  Metadata
            # alone is never sufficient: it must agree with both durable rows.
            child = self.orders.get(request.intent_id) or {}
            parent = self.orders.get(reservation_owner) or {}
            child_meta = dict(child.get("metadata", request.metadata) or {})
            staged_parent = str(child_meta.get("staged_parent_intent_id", "") or "")
            fallback_parent = str(child_meta.get("fallback_parent_intent_id", "") or "")
            relationship_parent = staged_parent or fallback_parent
            staged_lineage = (
                staged_parent == reservation_owner
                and self._staged_child_authorized(request, child, parent, reservation)
            )
            fallback_lineage = (
                fallback_parent == reservation_owner
                and str(parent.get("fallback_child_intent_id", "")) == request.intent_id
            )
            valid_child = (
                reservation_owner != request.intent_id
                and relationship_parent == reservation_owner
                and (staged_lineage or fallback_lineage)
                and str(parent.get("reservation_id", "")) == request.reservation_id
                and str(reservation.get("intent_id", "")) == reservation_owner
                and str(child.get("intent_id", "")) == request.intent_id
            )
            if not valid_child:
                return False, "capital_reservation_intent_mismatch", {}
        if str(reservation.get("candidate_id", "")) != request.candidate_id:
            return False, "capital_reservation_candidate_mismatch", {}
        now = time.time()
        signal_at = float(request.metadata.get("signal_timestamp", 0.0) or 0.0)
        max_age = float(_ss.get_val("opportunity_queue_ttl_secs", 300.0) or 300.0)
        if signal_at > 0 and now - signal_at > max(max_age, 1.0):
            return False, "candidate_expired_before_submission", {}
        try:
            from market_data_stream import execution_price_snapshot

            readiness = execution_price_snapshot(
                request.coin,
                float(_ss.get_val("execution_market_data_max_age_sec", 90.0) or 90.0),
            )
            current_price = float(readiness.get("price", 0.0) or 0.0)
            if not bool(readiness.get("fresh")) or current_price <= 0.0:
                # Under heavy engine load the WS consumer can be connected but
                # temporarily behind.  Preserve the fail-closed freshness gate,
                # but recover truth with one authoritative REST ticker rather
                # than rejecting a valid candidate solely because of WS backlog.
                ticker_getter = getattr(self.client, "get_execution_ticker", None)
                ticker = dict(
                    await (ticker_getter(request.coin) if ticker_getter else self.client.get_ticker(request.coin))
                    or {}
                )
                rest_price = float(ticker.get("last", 0.0) or 0.0)
                if rest_price <= 0.0:
                    return False, str(readiness.get("reason") or "market_price_unavailable"), readiness
                readiness = {
                    **readiness,
                    "stream_fresh": False,
                    "stream_reason": str(readiness.get("reason") or "market_price_unavailable"),
                    "stream_age_sec": readiness.get("age_sec"),
                    "source": "okx_rest_fallback",
                    "price": rest_price,
                    "latest_timestamp": time.time(),
                    "age_sec": 0.0,
                    "fresh": True,
                    "reason": "",
                }
                current_price = rest_price
        except Exception as exc:
            return False, f"market_freshness_check_failed:{type(exc).__name__}", {}
        try:
            try:
                account = await self.client.get_account_snapshot(force_refresh=True, priority="P1")
            except TypeError:
                account = await self.client.get_account_snapshot()
            available = float(account.get("avail_eq_usdt", 0.0) or 0.0)
            equity = float(
                account.get("total_eq_usdt", account.get("eq_usdt", available))
                or available
            )
            if available <= 0.0:
                return False, "account_available_balance_non_positive", {}
            try:
                config = await self.client.get_account_config(priority="P1")
            except TypeError:
                config = await self.client.get_account_config()
            position_mode = str(config.get("posMode", "") or "").lower()
            if position_mode not in {"long_short_mode", "net_mode"}:
                return False, f"invalid_position_mode:{position_mode or 'missing'}", {}
        except Exception as exc:
            return False, f"account_revalidation_failed:{type(exc).__name__}:{exc}", {}
        truth_ok, truth_reason, truth_detail = await self._fresh_position_truth()
        if not truth_ok:
            return False, truth_reason, truth_detail
        return True, "OK", {
            "validated_at": now,
            "available_balance_usdt": available,
            "equity_usdt": equity,
            "current_price": current_price,
            "instrument_id": instrument.inst_id,
            "position_mode": position_mode,
            "margin_mode": request.margin_mode,
            "leverage": int(request.leverage),
            "reservation_id": request.reservation_id,
            "market_freshness": readiness,
            **truth_detail,
        }

    async def submit(self, request: ExecutionRequest) -> ExecutionResult:
        # Restart dispatch is deliberately before *all* fallible live checks.
        # Once the mutation boundary was durably crossed, the intent is
        # exchange-owned work and can only be reconciled/read back, never
        # locally revalidated or reposted.
        existing_hint = self.orders.get(request.intent_id)
        if existing_hint:
            # A pre-lock lifecycle sample is only a scheduling hint.  The
            # holder may cross the exchange boundary while this caller waits,
            # so every terminal/recovery decision below uses the post-lock row.
            async with self._lock(request.intent_id):
                existing = self.orders.get(request.intent_id) or existing_hint
                state = str(existing.get("state", "") or "")
                if existing.get("terminal_finalized_at"):
                    await self._retry_terminal_cleanup(existing)
                    await self._flush_terminal_telemetry(request.intent_id)
                    return self._result_from_row(existing, "idempotent_existing_intent")
                if state == "PAYLOAD_READY":
                    row = await self._finalize_terminal(
                        request, state="FAILED_FINAL", terminal_class="local_rejection",
                        reason="recovery_payload_ready_never_submitted",
                        exchange_contacted=False,
                        fields={"recovery_only": True, "mutation_attempted": False},
                    )
                    self._release_pre_submit_registry(request.coin)
                    return self._result_from_row(row, "recovery_payload_ready_never_submitted", ok=False)
                if state in {"SUBMISSION_ATTEMPTED", "SUBMITTED", "ACKNOWLEDGED", "SUBMIT_UNCERTAIN", "RECOVERY_QUERY", "CANCEL_PENDING", "PARTIALLY_FILLED"}:
                    return await self._recover_uncertain(existing, request)
        if not self.paper_mode():
            if os.getenv("OXYPULSE_PRODUCTION_RESEARCH_ONLY", "false").strip().lower() == "true":
                return await self._reject_local(
                    request,
                    reason="production_research_runtime_execution_disabled",
                    funnel_counter="rejected_other",
                    stage="PRE_SUBMIT_REJECTED",
                )
            armed, reason = self.live_state.can_submit_live()
            if not armed:
                return await self._reject_local(
                    request,
                    reason=reason,
                    funnel_counter="rejected_account_state",
                    stage="PRE_SUBMIT_REJECTED",
                )
        try:
            request = await self._enrich_leverage_truth(request)
        except Exception as exc:
            return await self._reject_local(
                request,
                reason=f"leverage_truth_unavailable:{exc}",
                funnel_counter="rejected_account_state",
                stage="PRE_OKX_SKIP",
            )
        if request.order_type == "staged":
            return await self._submit_staged(request)
        async with self._lock(request.intent_id):
            # The pre-enrichment probe above is only an early recovery fast
            # path.  Admission ownership is decided here: another caller may
            # have created or crossed the durable mutation boundary while this
            # caller awaited enrichment.  Never create/reserve/POST twice.
            existing = self.orders.get(request.intent_id)
            if existing:
                state = str(existing.get("state", "") or "")
                if existing.get("terminal_finalized_at"):
                    await self._retry_terminal_cleanup(existing)
                    await self._flush_terminal_telemetry(request.intent_id)
                    return self._result_from_row(existing, "idempotent_existing_intent")
                # This is a second post-lock read after the fallible
                # enrichment phase.  A stale PAYLOAD_READY hint must never
                # override durable post-boundary ownership.
                if state == "PAYLOAD_READY":
                    row = await self._finalize_terminal(
                        request, state="FAILED_FINAL", terminal_class="local_rejection",
                        reason="recovery_payload_ready_never_submitted",
                        exchange_contacted=False,
                        fields={"recovery_only": True, "mutation_attempted": False},
                    )
                    self._release_pre_submit_registry(request.coin)
                    return self._result_from_row(row, "recovery_payload_ready_never_submitted", ok=False)
                if state in {"SUBMISSION_ATTEMPTED", "SUBMITTED", "ACKNOWLEDGED", "SUBMIT_UNCERTAIN", "RECOVERY_QUERY", "CANCEL_PENDING", "PARTIALLY_FILLED"}:
                    return await self._recover_uncertain(existing, request)
            client_order_id = deterministic_client_order_id(request.intent_id)
            self.orders.create({
                **request.__dict__,
                "client_order_id": client_order_id,
                "state": "INTENT_CREATED",
                "signal_at": float(request.metadata.get("signal_timestamp", 0.0) or 0.0),
                "candidate_at": float(request.metadata.get("candidate_timestamp", 0.0) or 0.0),
            })
            self.orders.transition(
                request.intent_id, "CAPITAL_RESERVED",
                reservation_id=request.reservation_id, reservation_at=time.time(),
            )
            if not await self._pin_reservation_for_router(
                request.reservation_id, reason="router_capital_reserved",
            ):
                row = await self._finalize_terminal(
                    request, state="FAILED_FINAL", terminal_class="local_rejection",
                    reason="capital_reservation_ownership_persistence_failed",
                    exchange_contacted=False,
                )
                self._release_pre_submit_registry(request.coin)
                return self._result_from_row(
                    row, "capital_reservation_ownership_persistence_failed", ok=False,
                )
            self.orders.transition(request.intent_id, "ROUTER_VALIDATING", router_at=time.time())
            _flow.record(
                "ROUTER_REQUESTED", intent_id=request.intent_id, candidate_id=request.candidate_id,
                reservation_id=request.reservation_id, engine=request.engine, coin=request.coin,
                direction=request.direction, funnel_counter="router_requested",
            )
            validation: dict = {}
            if not self.paper_mode():
                if "PYTEST_CURRENT_TEST" not in os.environ:
                    valid, validation_reason, validation = await self._validate_live_submission(request)
                    if not valid:
                        counter = _exec_min.classify_pre_okx_reason(validation_reason)
                        local_truth_skip = validation_reason in {
                            "position_truth_unavailable",
                            "account_truth_stale",
                            "position_truth_mismatch",
                        }
                        if local_truth_skip:
                            _flow.record_pre_okx_skip(
                                intent_id=request.intent_id, candidate_id=request.candidate_id,
                                reservation_id=request.reservation_id, engine=request.engine, coin=request.coin,
                                direction=request.direction, reason=validation_reason, validation=validation,
                                funnel_counter=counter,
                            )
                        else:
                            _flow.record(
                                "PRE_SUBMIT_REJECTED", intent_id=request.intent_id, candidate_id=request.candidate_id,
                                reservation_id=request.reservation_id, engine=request.engine, coin=request.coin,
                                direction=request.direction, reason=validation_reason, validation=validation,
                                funnel_counter=counter, terminal_class=counter, exchange_contacted=False,
                            )
                        row = await self._finalize_terminal(
                            request, state="REJECTED", terminal_class=counter,
                            reason=validation_reason, exchange_contacted=False,
                            fields={"rejection_reason": validation_reason,
                                    "terminal_family": "local_rejection",
                                    "live_submission_validation": validation,
                                    "funnel_counter": counter},
                        )
                        self._release_pre_submit_registry(request.coin)
                        return self._result_from_row(row, validation_reason, ok=False)
                    self.orders.transition(
                        request.intent_id, "ROUTER_VALIDATING",
                        live_submission_validation=validation,
                    )
                    if validation.get("position_mode"):
                        request.metadata["position_mode"] = str(validation["position_mode"])
                    _flow.record(
                        "ROUTER_VALIDATED", intent_id=request.intent_id, candidate_id=request.candidate_id,
                        reservation_id=request.reservation_id, engine=request.engine, coin=request.coin,
                        direction=request.direction, funnel_counter="router_validated",
                        validation=validation,
                    )
                try:
                    self.instruments.require(request.coin)
                except Exception as exc:
                    return await self._reject_local(
                        request,
                        reason=str(exc),
                        funnel_counter=_exec_min.classify_pre_okx_reason(str(exc)),
                        stage="PRE_OKX_SKIP",
                        client_order_id=client_order_id,
                    )
                if _controlled_lifecycle.enforced():
                    claimed, claim_reason = _controlled_lifecycle.claim(
                        request.intent_id,
                        {
                            "candidate_id": request.candidate_id,
                            "engine": request.engine,
                            "coin": request.coin,
                            "direction": request.direction,
                            "requested_contracts": request.requested_contracts,
                            "approved_contracts": request.approved_contracts,
                            "approved_notional_usdt": request.approved_notional_usdt,
                            "order_type": request.order_type,
                            "arrival_price": request.price,
                            "stop_price": request.stop_price,
                            "take_profit": request.take_profit,
                            "margin_mode": request.margin_mode,
                            "leverage": request.leverage,
                            "metadata": dict(request.metadata or {}),
                        },
                    )
                    if not claimed:
                        return await self._reject_local(
                            request,
                            reason=claim_reason,
                            funnel_counter="rejected_other",
                            stage="PRE_OKX_SKIP",
                            client_order_id=client_order_id,
                        )

            # Deterministic local executability: same floor as place_market_order.
            fresh_balance = None
            if not self.paper_mode():
                fresh_balance = (
                    validation.get("available_balance_usdt")
                    or validation.get("equity_usdt")
                )
            local_ok, local_reason, local_detail = self._local_executable_check(
                request,
                balance_usdt=fresh_balance,
            )
            if not local_ok:
                counter = str(local_detail.get("funnel_counter") or _exec_min.classify_pre_okx_reason(local_reason))
                return await self._reject_local(
                    request,
                    reason=local_reason,
                    funnel_counter=counter,
                    stage="PRE_OKX_SKIP",
                    extra={"local_executable_check": local_detail},
                    client_order_id=client_order_id,
                )

            if not self.paper_mode():
                readiness = getattr(self.client, "mutation_transport_ready", None)
                if callable(readiness):
                    ready, readiness_reason = await readiness()
                    if not ready:
                        return await self._reject_local(
                            request,
                            reason=f"mutation_transport_not_ready:{readiness_reason}",
                            funnel_counter="rejected_other",
                            stage="PRE_OKX_SKIP",
                            client_order_id=client_order_id,
                        )

            # PAYLOAD_READY means local deterministic execution requirements
            # passed. The durable mutation marker is written later by the
            # before_send callback, after rate/pool admission and immediately
            # before the real transport call.
            payload_ready_at = time.time()
            self.orders.transition(
                request.intent_id, "PAYLOAD_READY",
                payload_ready_at=payload_ready_at,
                placement_min_notional_usdt=local_detail.get("placement_min_notional_usdt"),
                local_executable_check=local_detail,
            )
            _flow.record(
                "PAYLOAD_READY", intent_id=request.intent_id, candidate_id=request.candidate_id,
                reservation_id=request.reservation_id, engine=request.engine, coin=request.coin,
                direction=request.direction, order_type=request.order_type,
                approved_contracts=request.approved_contracts,
                approved_notional_usdt=request.approved_notional_usdt,
                placement_min_notional_usdt=local_detail.get("placement_min_notional_usdt"),
                funnel_counter="payload_ready",
            )
            exchange_request_started = False
            lifecycle_persisted = False

            def mark_exchange_request() -> None:
                nonlocal exchange_request_started, lifecycle_persisted
                if exchange_request_started:
                    return
                # This callback runs only after rate and pool admission, at the
                # actual request boundary. Persisting the attempted lifecycle
                # is therefore part of the no-POST-before-durability guard.
                if not self.paper_mode():
                    from position_registry import update as _registry_update

                    try:
                        updated = _registry_update(
                            request.coin,
                            metadata={
                                "entry_lifecycle": "SUBMISSION_ATTEMPTED",
                                "mutation_attempted": True,
                                "execution_intent_id": request.intent_id,
                                "client_order_id": client_order_id,
                                "submission_attempted_at": time.time(),
                            },
                            require_persist=True,
                        )
                    except Exception as exc:
                        raise RuntimeError(
                            f"lifecycle_persistence_failed:{type(exc).__name__}:{exc}"
                        ) from exc
                    if not updated:
                        raise RuntimeError("lifecycle_persistence_failed:pre_submit_registry_missing")
                    lifecycle_persisted = True
                if not self._pin_reservation_at_boundary(
                    request.reservation_id,
                    state="SUBMISSION_ATTEMPTED",
                    reason="router_submission_boundary",
                ):
                    raise RuntimeError("capital_reservation_submission_ownership_persistence_failed")
                self.orders.transition(
                    request.intent_id, "SUBMISSION_ATTEMPTED",
                    exchange_request_at=time.time(), client_order_id=client_order_id,
                )
                _flow.record(
                    "EXCHANGE_SUBMIT_ATTEMPTED", intent_id=request.intent_id, candidate_id=request.candidate_id,
                    reservation_id=request.reservation_id, engine=request.engine, coin=request.coin,
                    direction=request.direction, client_order_id=client_order_id,
                    funnel_counter="submission_attempted",
                )
                exchange_request_started = True
            try:
                response = await self._submit_once(request, client_order_id, mark_exchange_request)
            except asyncio.CancelledError:
                if exchange_request_started:
                    self._mark_recovery_pending(
                        request, state="SUBMIT_UNCERTAIN",
                        reason="submission_task_cancelled_after_request_started",
                        submit_error="CancelledError:submission_task_cancelled_after_request_started",
                        client_order_id=client_order_id, exchange_contacted=True,
                    )
                else:
                    await self._finalize_terminal(
                        request, state="REJECTED", terminal_class="local_rejection",
                        reason="submission_task_cancelled_before_exchange_request",
                        exchange_contacted=False,
                        fields={"rejection_reason": "submission_task_cancelled_before_exchange_request"},
                    )
                    _flow.record_pre_okx_skip(
                        intent_id=request.intent_id, candidate_id=request.candidate_id,
                        reservation_id=request.reservation_id, engine=request.engine, coin=request.coin,
                        direction=request.direction,
                        reason="submission_task_cancelled_before_exchange_request",
                        funnel_counter="rejected_other",
                    )
                    self._release_pre_submit_registry(request.coin)
                raise
            except Exception as exc:
                if not exchange_request_started:
                    detail = str(exc)
                    reason = detail if detail.startswith("lifecycle_persistence_failed:") else (
                        f"pre_submit_failure:{type(exc).__name__}:{exc}"
                    )
                    counter = _exec_min.classify_pre_okx_reason(reason)
                    row = await self._finalize_terminal(
                        request, state="REJECTED", terminal_class=counter,
                        reason=reason, exchange_contacted=False,
                        fields={"terminal_family": "local_rejection",
                                "rejection_reason": reason, "funnel_counter": counter},
                    )
                    _flow.record(
                        "PRE_OKX_SKIP", intent_id=request.intent_id, candidate_id=request.candidate_id,
                        reservation_id=request.reservation_id, engine=request.engine, coin=request.coin,
                        direction=request.direction, reason=reason,
                        client_order_id=client_order_id, funnel_counter=counter,
                        terminal_class=counter, exchange_contacted=False,
                    )
                    self._release_pre_submit_registry(request.coin)
                    return self._result_from_row(row, reason, ok=False)
                if exc.__class__.__name__ == "OKXDefinitiveRejection":
                    reason = f"exchange_rejection:{exc}"
                    row = await self._finalize_terminal(
                        request, state="REJECTED", terminal_class="exchange_rejection",
                        reason=reason, exchange_contacted=True,
                        fields={
                            "rejection_reason": reason,
                            "mutation_evidence": dict(getattr(exc, "evidence", {}) or {}),
                            "client_order_id": client_order_id,
                        },
                    )
                    _flow.record(
                        "EXCHANGE_REJECTED", intent_id=request.intent_id,
                        candidate_id=request.candidate_id,
                        reservation_id=request.reservation_id, engine=request.engine,
                        coin=request.coin, direction=request.direction,
                        client_order_id=client_order_id,
                        funnel_counter="exchange_rejected",
                    )
                    self._release_pre_submit_registry(request.coin)
                    return self._result_from_row(row, reason, ok=False)
                if not self.paper_mode() and _controlled_lifecycle.enforced():
                    _controlled_lifecycle.mark_submission(
                        intent_id=request.intent_id,
                        state="SUBMIT_UNCERTAIN",
                        reason=f"{type(exc).__name__}:{exc}",
                    )
                    self.live_state.disarm("controlled_lifecycle_submission_consumed")
                message = str(exc).lower()
                if any(token in message for token in ("lot", "tick", "minimum size", "51000", "51008", "51020")):
                    try:
                        await self.instruments.refresh_after_metadata_error(self.client)
                    except Exception:
                        pass
                row = self._mark_recovery_pending(
                    request, state="SUBMIT_UNCERTAIN", reason=f"{type(exc).__name__}:{exc}",
                    submit_error=f"{type(exc).__name__}:{exc}", client_order_id=client_order_id,
                    exchange_contacted=True,
                )
                return self._result_from_row(row, "submit_uncertain", ok=False)
            if isinstance(response, dict) and response.get("post_attempted") is False:
                reason = str(response.get("reason", "") or "pre_okx_skip:post_attempted_false")
                if not reason.startswith("pre_okx_skip:"):
                    reason = f"pre_okx_skip:{reason}"
                return await self._reject_local(
                    request,
                    reason=reason,
                    funnel_counter="rejected_other",
                    stage="PRE_OKX_SKIP",
                    extra={
                        "post_attempted": False,
                        "mutation_attempted": False,
                        "physical_post_count": 0,
                        "mutation_evidence": dict(response),
                    },
                    client_order_id=client_order_id,
                )
            if not response:
                skip_reason = ""
                try:
                    consume = getattr(self.client, "consume_last_order_skip_reason", None)
                    if callable(consume):
                        skip_reason = str(consume() or "")
                except Exception:
                    skip_reason = ""
                if not skip_reason:
                    try:
                        skip_reason = str(getattr(self.client, "_last_order_skip_reason", "") or "")
                    except Exception:
                        skip_reason = ""
                if not exchange_request_started:
                    reason = skip_reason or "pre_okx_skip:empty_response_before_exchange_request"
                    counter = _exec_min.classify_pre_okx_reason(reason)
                    row = await self._finalize_terminal(
                        request, state="REJECTED", terminal_class=counter,
                        reason=reason, exchange_contacted=False,
                        fields={"terminal_family": "local_rejection",
                                "rejection_reason": reason, "funnel_counter": counter},
                    )
                    _flow.record_pre_okx_skip(
                        intent_id=request.intent_id, candidate_id=request.candidate_id,
                        reservation_id=request.reservation_id, engine=request.engine, coin=request.coin,
                        direction=request.direction, reason=reason, funnel_counter=counter,
                        client_order_id=client_order_id,
                        approved_contracts=request.approved_contracts,
                        approved_notional_usdt=request.approved_notional_usdt,
                    )
                    self._release_pre_submit_registry(request.coin)
                    try:
                        from capital_scheduler import scheduler
                        scheduler.notify("order_rejected")
                    except Exception:
                        pass
                    return self._result_from_row(row, reason, ok=False)
                # A post-boundary empty response is transport ambiguity, never
                # evidence of rejection.  Recovery is read-only and cannot POST.
                if not self.paper_mode() and _controlled_lifecycle.enforced():
                    _controlled_lifecycle.mark_submission(
                        intent_id=request.intent_id, state="SUBMIT_UNCERTAIN", reason="exchange_uncertain"
                    )
                    self.live_state.disarm("controlled_lifecycle_submission_consumed")
                reason = skip_reason or "exchange_rejected:empty_response_after_request"
                row = self._mark_recovery_pending(
                    request, state="SUBMIT_UNCERTAIN", reason=reason,
                    submit_error=reason, client_order_id=client_order_id,
                    exchange_contacted=True,
                )
                return self._result_from_row(row, "submit_uncertain", ok=False)
            order_id = str(response.get("ordId", "") or "")
            if not self.paper_mode() and _controlled_lifecycle.enforced():
                _controlled_lifecycle.mark_submission(
                    intent_id=request.intent_id, order_id=order_id, state="SUBMITTED", reason="exchange_acknowledged"
                )
                self.live_state.disarm("controlled_lifecycle_submission_consumed")
            row = self.orders.transition(
                request.intent_id, "SUBMITTED", exchange_order_id=order_id,
                response=response, exchange_ack_at=time.time(),
            )
            # A staged child contributes to its parent's single resulting
            # position slot.  Persist the shared operation on both the child
            # and its durable parent before touching the idempotent sink.
            if not self._ensure_recovered_open_risk_slot(request.intent_id):
                # The exchange acknowledgement means this entry may exist, but
                # its matching local risk ledger OPEN is not durable yet.  Do
                # not read, protect, terminalize, or release it: recovery owns
                # the intent until the deterministic operation completes.
                row = self._mark_recovery_pending(
                    request,
                    state="SUBMITTED",
                    reason="submitted_risk_slot_open_pending",
                    exchange_order_id=order_id,
                    response=response,
                    exchange_contacted=True,
                    recovery_evidence={"acknowledgement": response},
                )
                return self._result_from_row(row, "submitted_risk_slot_open_pending", ok=False)
            _flow.record(
                "EXCHANGE_ACKNOWLEDGED", intent_id=request.intent_id, candidate_id=request.candidate_id,
                reservation_id=request.reservation_id, engine=request.engine, coin=request.coin,
                direction=request.direction, order_id=order_id, client_order_id=client_order_id,
                funnel_counter="exchange_acknowledged",
            )
            if self.paper_mode():
                fill_qty = float(response.get("contracts", response.get("filled_contracts", request.approved_contracts)) or request.approved_contracts)
                fill_px = float(response.get("fillPx", request.price) or request.price)
                self.orders.add_fill(request.intent_id, {"fillId": f"PAPER-{request.intent_id}", "fillSz": fill_qty, "fillPx": fill_px, "fee": 0.0})
                # Preserve exchange/paper fill truth while required protection
                # work runs; terminal taxonomy is chosen only after it returns.
                row = self.orders.transition(
                    request.intent_id, "PARTIALLY_FILLED",
                    filled_contracts=fill_qty, average_fill_price=fill_px,
                    exchange_fill_confirmed=True, order_status={"state": "filled", "paper": True},
                )
            else:
                try:
                    row = await self._confirm_live_state(request, order_id, client_order_id)
                    if row.get("state") == "SUBMIT_UNCERTAIN" and request.order_type == "limit" and request.fallback_mode == "market":
                        row = await self._limit_timeout_fallback(request, order_id, client_order_id)
                except Exception as exc:
                    # The mutation was acknowledged.  Any later local/readback
                    # failure is recovery work, never permission to resend.
                    row = self._mark_recovery_pending(
                        request, state="SUBMIT_UNCERTAIN",
                        reason=f"post_acknowledgement_exception:{type(exc).__name__}:{exc}",
                        submit_error=f"post_acknowledgement_exception:{type(exc).__name__}:{exc}",
                        exchange_order_id=order_id, response=response,
                        exchange_contacted=True,
                    )
            # Authoritative partial-cancel convergence is already terminal and
            # has completed registry/protection accounting.  Never run the
            # generic fill path a second time (which could duplicate protection
            # attachment after a direct cancellation readback).
            if row.get("terminal_finalized_at"):
                final = self.orders.get(request.intent_id) or row
                return self._result_from_row(
                    final, str(final.get("terminal_reason", final.get("state", "terminal"))), ok=False,
                )
            # ``_confirm_live_state`` has already made exactly one protection
            # attempt for this observed partial fill.  A live remainder keeps
            # the intent recovery-owned; do not enter the generic filled path
            # below, which would issue a second protection request and then
            # terminalize/release the parent reservation.
            if (
                str(row.get("state", "") or "") == "PARTIALLY_FILLED"
                and bool(row.get("recovery_required"))
                and str(row.get("recovery_reason", "") or "") == "live_partial_protection_failure"
            ):
                return self._result_from_row(row, "live_partial_protection_failure", ok=False)
            filled = float(row.get("filled_contracts", 0.0) or 0.0)
            if filled > 0:
                _flow.record(
                    "FILLED", intent_id=request.intent_id, candidate_id=request.candidate_id,
                    reservation_id=request.reservation_id, engine=request.engine, coin=request.coin,
                    direction=request.direction, order_id=order_id, filled_contracts=filled,
                    average_fill_price=float(row.get("average_fill_price", 0.0) or 0.0),
                    funnel_counter="filled",
                )
            protection_result = None
            registry_activated = False
            registry_activation_error = ""
            if filled > 0:
                leverage_truth = await self._position_effective_leverage(request)
                position_effective = leverage_truth.get("position_effective_leverage")
                # A filled live position without a successful effective-leverage
                # read remains explicitly unknown.  Configured/requested values
                # stay historical pre-submit evidence and must not become live
                # ROI truth by fallback.
                if not position_effective or int(position_effective) <= 0:
                    row = self._mark_recovery_pending(
                        request,
                        state="PARTIALLY_FILLED",
                        reason="live_fill_effective_leverage_unknown",
                        filled_contracts=filled,
                        average_fill_price=float(row.get("average_fill_price", 0.0) or 0.0),
                        exchange_order_id=order_id,
                        cumulative_fill_evidence=FILL_KNOWN_POSITIVE,
                        recovery_evidence={"leverage_truth": leverage_truth},
                    )
                    return self._result_from_row(row, "live_fill_effective_leverage_unknown", ok=False)
                lifecycle_leverage = int(position_effective)
                request = replace(
                    request,
                    leverage=lifecycle_leverage,
                    position_effective_leverage=position_effective,
                    leverage_truth_status=str(leverage_truth.get("leverage_truth_status", request.leverage_truth_status) or request.leverage_truth_status),
                    leverage_truth_timestamp=str(leverage_truth.get("leverage_truth_timestamp", request.leverage_truth_timestamp) or request.leverage_truth_timestamp),
                    leverage_truth_source=str(leverage_truth.get("leverage_truth_source", request.leverage_truth_source) or request.leverage_truth_source),
                    leverage_truth_mismatch=bool(leverage_truth.get("leverage_truth_mismatch", request.leverage_truth_mismatch)),
                    requested_vs_configured_mismatch=bool(
                        leverage_truth.get(
                            "requested_vs_configured_mismatch",
                            request.requested_vs_configured_mismatch,
                        )
                    ),
                    configured_vs_effective_mismatch=bool(
                        leverage_truth.get("configured_vs_effective_mismatch", False)
                    ),
                    metadata={**dict(request.metadata or {}), **leverage_truth},
                )
                row = self.orders.transition(
                    request.intent_id,
                    row.get("state", "FILLED"),
                    requested_leverage=request.requested_leverage,
                    exchange_configured_leverage=request.exchange_configured_leverage,
                    position_effective_leverage=position_effective,
                    leverage_truth_status=request.leverage_truth_status,
                    leverage_truth_timestamp=request.leverage_truth_timestamp,
                    leverage_truth_source=str(leverage_truth.get("leverage_truth_source", "") or ""),
                    leverage_truth_mismatch=bool(leverage_truth.get("leverage_truth_mismatch", False)),
                    requested_vs_configured_mismatch=bool(
                        leverage_truth.get("requested_vs_configured_mismatch", False)
                    ),
                    configured_vs_effective_mismatch=bool(
                        leverage_truth.get("configured_vs_effective_mismatch", False)
                    ),
                    leverage=lifecycle_leverage,
                )
                leverage_metadata = {
                    "requested_leverage": request.requested_leverage,
                    "exchange_configured_leverage": request.exchange_configured_leverage,
                    "position_effective_leverage": position_effective,
                    "leverage_truth_status": request.leverage_truth_status,
                    "leverage_truth_timestamp": request.leverage_truth_timestamp,
                    "leverage_truth_source": str(leverage_truth.get("leverage_truth_source", "") or ""),
                    "leverage_truth_mismatch": bool(leverage_truth.get("leverage_truth_mismatch", False)),
                    "requested_vs_configured_mismatch": bool(
                        leverage_truth.get("requested_vs_configured_mismatch", False)
                    ),
                    "configured_vs_effective_mismatch": bool(
                        leverage_truth.get("configured_vs_effective_mismatch", False)
                    ),
                }
                # The protection transaction persists its intent into the
                # filled registry row before any protection POST.  Activate
                # the durable filled row first; the protection result is
                # written back below after the exchange readback completes.
                try:
                    from position_registry import (
                        derive_exchange_lifecycle_anchor,
                        update as _registry_update,
                    )
                    _fill_price = float(row.get("average_fill_price", request.price) or request.price)
                    _instrument = self.instruments.require(request.coin) if not self.paper_mode() else self.instruments.get(request.coin)
                    _filled_notional = (
                        _instrument.contracts_to_notional(filled, _fill_price)
                        if _instrument is not None and filled > 0 and _fill_price > 0
                        else float(request.approved_notional_usdt)
                    )
                    updated = _registry_update(
                        request.coin,
                        size_usdt=_filled_notional,
                        entry=_fill_price,
                        leverage=lifecycle_leverage,
                        stop_loss=float(request.stop_price),
                        take_profit=float(request.take_profit),
                        order_id=order_id,
                        metadata={
                            "origin_entry_order_id": order_id,
                            "origin_entry_client_id": client_order_id,
                            "exchange_position_evidence": dict(
                                request.metadata.get("exchange_position_evidence", {}) or {}
                            ),
                            "exchange_lifecycle_anchor": derive_exchange_lifecycle_anchor(
                                request.metadata.get("exchange_position_evidence", {})
                            ),
                            "pending_reservation": False,
                            "entry_lifecycle": "FILLED",
                            "mutation_attempted": True,
                            "capital_reservation_id": request.reservation_id,
                            "execution_intent_id": request.intent_id,
                            "risk_slot_intent_id": self._risk_slot_owner_intent(request.intent_id),
                            "candidate_id": request.candidate_id,
                            **leverage_metadata,
                            "exchange_confirmed": not self.paper_mode(),
                            "desired_stop_price": float(request.stop_price or 0.0),
                            "exchange_confirmed_stop_price": 0.0 if not self.paper_mode() else float(request.stop_price or 0.0),
                            "exchange_protection_status": "PROTECTION_INTENT_PENDING" if not self.paper_mode() else "PAPER_LOCAL",
                            "tp_owner": str(request.metadata.get("tp_owner", "") or ""),
                            "tp_mode_at_entry": str(request.metadata.get("tp_mode_at_entry", "") or ""),
                            "local_tp1_enabled": bool(request.metadata.get("local_tp1_enabled", False)),
                            "local_tp2_enabled": bool(request.metadata.get("local_tp2_enabled", False)),
                            "tp_owner_timestamp": str(request.metadata.get("tp_owner_timestamp", request.metadata.get("ownership_timestamp", "")) or ""),
                            "partials_enabled_at_entry": bool(request.metadata.get("partials_enabled_at_entry", False)),
                        },
                        require_persist=True,
                    )
                    if not updated:
                        raise RuntimeError("durable_filled_registry_activation_not_confirmed")
                    registry_activated = True
                except Exception as exc:
                    logger.error("Durable filled registry activation failed before protection %s: %s", request.intent_id, exc)
                    registry_activation_error = f"{type(exc).__name__}:{exc}"
                if registry_activation_error:
                    row = self._mark_recovery_pending(
                        request, state="PARTIALLY_FILLED",
                        reason="filled_registry_activation_pending",
                        filled_contracts=filled,
                        average_fill_price=float(row.get("average_fill_price", 0.0) or 0.0),
                        exchange_order_id=order_id,
                        registry_activation_error=registry_activation_error,
                        recovery_evidence={"order": dict(row.get("order_status", {}) or {})},
                    )
                    return self._result_from_row(row, "filled_registry_activation_pending", ok=False)
                try:
                    protection_result = await self.protection.ensure(
                        coin=request.coin,
                        direction=request.direction,
                        contracts=filled,
                        stop_price=request.stop_price,
                        take_profit=request.take_profit,
                        client_order_id=client_order_id,
                        paper=self.paper_mode(),
                        tp_owner=str(request.metadata.get("tp_owner", "") or ""),
                    )
                except Exception as exc:
                    row = await self._finalize_terminal(
                        request, state="FILLED", terminal_class="protection_failure",
                        reason=f"protection_exception_after_fill:{type(exc).__name__}:{exc}",
                        exchange_contacted=True,
                        fields={"filled_contracts": filled,
                                "average_fill_price": float(row.get("average_fill_price", 0.0) or 0.0),
                                "protection": {"protected": False, "reason": f"{type(exc).__name__}:{exc}"}},
                    )
                    return self._result_from_row(row, "protection_failure", ok=False)
                self.orders.transition(
                    request.intent_id, row.get("state", "FILLED"),
                    protection=protection_result.__dict__,
                    protection_ack_at=time.time(),
                    requested_vs_configured_mismatch=bool(
                        leverage_metadata.get("requested_vs_configured_mismatch", False)
                    ),
                    configured_vs_effective_mismatch=bool(
                        leverage_metadata.get("configured_vs_effective_mismatch", False)
                    ),
                )
                if protection_result.protected:
                    try:
                        from position_registry import update as _registry_update
                        _fill_price = float(row.get("average_fill_price", request.price) or request.price)
                        _instrument = self.instruments.require(request.coin) if not self.paper_mode() else self.instruments.get(request.coin)
                        _filled_notional = (
                            _instrument.contracts_to_notional(filled, _fill_price)
                            if _instrument is not None and filled > 0 and _fill_price > 0
                            else float(request.approved_notional_usdt)
                        )
                        updated = _registry_update(
                            request.coin,
                            size_usdt=_filled_notional,
                            entry=_fill_price,
                            leverage=lifecycle_leverage,
                            stop_loss=float(request.stop_price),
                            take_profit=float(request.take_profit),
                            order_id=order_id,
                            metadata={
                                "pending_reservation": False,
                                "entry_lifecycle": "FILLED",
                                "mutation_attempted": True,
                                "capital_reservation_id": request.reservation_id,
                                "execution_intent_id": request.intent_id,
                                "risk_slot_intent_id": self._risk_slot_owner_intent(request.intent_id),
                                "candidate_id": request.candidate_id,
                                **leverage_metadata,
                                "exchange_confirmed": not self.paper_mode(),
                                "desired_stop_price": float(request.stop_price or 0.0),
                                "local_effective_stop_price": float(
                                    protection_result.confirmed_stop_price or 0.0
                                ) if not self.paper_mode() else float(request.stop_price or 0.0),
                                "exchange_confirmed_stop_price": float(
                                    protection_result.confirmed_stop_price or 0.0
                                ),
                                "exchange_protection_status": (
                                    "STOP_CONFIRMED" if protection_result.protected and not self.paper_mode()
                                    else "PAPER_LOCAL" if self.paper_mode() else str(
                                        protection_result.reason or protection_result.protection_state or ""
                                    )
                                ),
                            "exchange_protection_algo_ids": [
                                value for value in [
                                    protection_result.sl_algo_id,
                                    *list(protection_result.tp_algo_ids or []),
                                ] if value
                            ],
                            "exchange_tp_algo_id": str((protection_result.tp_algo_ids or [""])[0] or ""),
                            "tp_owner": str(request.metadata.get("tp_owner", "") or ""),
                            "tp_mode_at_entry": str(request.metadata.get("tp_mode_at_entry", "") or ""),
                            "local_tp1_enabled": bool(request.metadata.get("local_tp1_enabled", False)),
                            "local_tp2_enabled": bool(request.metadata.get("local_tp2_enabled", False)),
                            "tp_owner_timestamp": str(request.metadata.get("tp_owner_timestamp", request.metadata.get("ownership_timestamp", "")) or ""),
                                "partials_enabled_at_entry": bool(request.metadata.get("partials_enabled_at_entry", False)),
                            },
                            require_persist=True,
                        )
                        if not updated:
                            raise RuntimeError("durable_filled_registry_protection_lineage_not_confirmed")
                        registry_activated = True
                    except Exception as exc:
                        logger.error("Registry activation failed after fill %s: %s", request.intent_id, exc)
                        row = self._mark_recovery_pending(
                            request, state="PARTIALLY_FILLED",
                            reason="filled_registry_activation_pending",
                            filled_contracts=filled,
                            average_fill_price=float(row.get("average_fill_price", 0.0) or 0.0),
                            exchange_order_id=order_id,
                            protection=protection_result.__dict__,
                            registry_activation_error=f"{type(exc).__name__}:{exc}",
                            recovery_evidence={"protection": protection_result.__dict__},
                        )
                        return self._result_from_row(row, "filled_registry_activation_pending", ok=False)
            terminal_state = str(row.get("state", "") or "")
            if filled > 0 and terminal_state != "CANCEL_PENDING":
                terminal_class = "protection_failure" if protection_result is not None and not protection_result.protected else "filled"
                row = await self._finalize_terminal(
                    request, state="FILLED" if terminal_state != "PARTIALLY_FILLED" else "FILLED",
                    terminal_class=terminal_class,
                    reason="protection_failure_after_fill" if terminal_class == "protection_failure" else "filled",
                    exchange_contacted=True,
                    fields={"filled_contracts": filled,
                            "average_fill_price": float(row.get("average_fill_price", 0.0) or 0.0),
                            "protection": getattr(protection_result, "__dict__", row.get("protection", {}))},
                )
            elif terminal_state == "CANCELLED":
                row = await self._finalize_terminal(
                    request, state="CANCELLED", terminal_class="cancelled", reason="acknowledged_unfilled_cancelled",
                    exchange_contacted=True,
                )
            elif terminal_state == "REJECTED":
                row = await self._finalize_terminal(
                    request, state="REJECTED", terminal_class="exchange_rejection",
                    reason=str(row.get("rejection_reason", "exchange_rejection") or "exchange_rejection"),
                    exchange_contacted=True,
                )
            elif terminal_state in {"SUBMIT_UNCERTAIN", "ACKNOWLEDGED", "SUBMITTED"}:
                row = self._mark_recovery_pending(
                    request, state=terminal_state,
                    reason=str(row.get("submit_error", "acknowledged_unfilled") or "acknowledged_unfilled"),
                )
            if row.get("terminal_finalized_at") or (
                row.get("state") in {"FILLED", "PARTIALLY_FILLED"} and registry_activated
            ):
                try:
                    from capital_scheduler import scheduler
                    scheduler.notify("reservation_released")
                except Exception:
                    pass
            final = self.orders.get(request.intent_id) or row
            ok = (
                final.get("state") in {"FILLED", "PARTIALLY_FILLED"}
                and final.get("state") != "CANCEL_PENDING"
                and (protection_result is None or protection_result.protected)
            )
            return self._result_from_row(final, "executed" if ok else str(final.get("state", "failed")), ok=ok)

    async def _submit_staged(self, request: ExecutionRequest) -> ExecutionResult:
        instrument = self.instruments.require(request.coin)
        chunks = list(request.metadata.get("execution_chunks", []) or [])
        if not chunks:
            chunks = [{"fraction": 0.40, "price_offset_pct": 0.0}, {"fraction": 0.35, "price_offset_pct": -0.001}, {"fraction": 0.25, "price_offset_pct": -0.002}]
        split_floor = max(float(instrument.min_sz or 0.0), float(instrument.lot_sz or 0.0))
        configured_split_floor = float(_ss.get_val("execution_split_min_contracts", 0.0) or 0.0)
        split_floor = max(split_floor * len(chunks), configured_split_floor)
        if split_floor > 0.0 and float(request.approved_contracts or 0.0) < split_floor:
            # A small executable order is safer as one market mutation than as
            # child orders that can each fall below minSz or add fee drag.
            return await self.submit(
                replace(
                    request,
                    order_type="market",
                    metadata={**dict(request.metadata or {}), "small_order_unsplit": True},
                )
            )
        # Parent admission is also intent-serialized.  The manifest is saved
        # before any child row exists, so a restart has parent-owned authority
        # to reconstruct the exact child set without trusting child metadata.
        async with self._lock(request.intent_id):
            existing = self.orders.get(request.intent_id)
            if existing:
                if existing.get("terminal_finalized_at"):
                    return self._result_from_row(existing, "idempotent_existing_intent")
                return await self._resume_staged_parent(request, existing)
            manifest = self._staged_manifest(request, instrument, chunks)
            self.orders.create({
                **request.__dict__, "client_order_id": deterministic_client_order_id(request.intent_id),
                "state": "INTENT_CREATED", "staged_manifest": manifest,
                "reservation_owner_intent_id": request.intent_id,
            })
            existing = self.orders.get(request.intent_id) or {}
        return await self._resume_staged_parent(request, existing)

    async def _resume_staged_parent(self, request: ExecutionRequest, parent: dict) -> ExecutionResult:
        """Resume only durable, parent-authorized staged children after restart."""
        manifest = dict(parent.get("staged_manifest", {}) or {})
        entries = list(manifest.get("children", []) or [])
        if (
            str(manifest.get("staged_parent_intent_id", "") or "") != request.intent_id
            or str(manifest.get("reservation_owner_intent_id", "") or "") != request.intent_id
            or str(manifest.get("reservation_id", "") or "") != request.reservation_id
            or not entries
        ):
            child_evidence = any(
                bool(row.get("mutation_attempted"))
                or str(row.get("state", "") or "") in {
                    "SUBMISSION_ATTEMPTED", "SUBMITTED", "ACKNOWLEDGED", "SUBMIT_UNCERTAIN",
                    "RECOVERY_QUERY", "CANCEL_PENDING", "PARTIALLY_FILLED", "FILLED", "CANCELLED",
                    "REJECTED", "EXPIRED", "FAILED_FINAL",
                }
                for row in self.orders.intent_prefix(f"{request.intent_id}-S")
            )
            if not child_evidence:
                row = await self._finalize_terminal(
                    request, state="FAILED_FINAL", terminal_class="local_rejection",
                    reason="staged_manifest_missing_before_child_boundary", exchange_contacted=False,
                    fields={"recovery_only": True, "staged_manifest_error": "missing_or_invalid"},
                )
                return self._result_from_row(row, "staged_manifest_missing_before_child_boundary", ok=False)
            self.orders.transition(
                request.intent_id, "INTENT_CREATED", recovery_required=True,
                recovery_reason="staged_manifest_missing_with_child_evidence",
            )
            return self._result_from_row(parent, "staged_manifest_missing_with_child_evidence", ok=False)

        for entry in entries:
            child_request = self._staged_child_request(request, parent, dict(entry))
            child = self.orders.get(child_request.intent_id)
            if child and child.get("terminal_finalized_at"):
                continue
            if child:
                state = str(child.get("state", "") or "")
                if state in {"SUBMISSION_ATTEMPTED", "SUBMITTED", "ACKNOWLEDGED", "SUBMIT_UNCERTAIN", "RECOVERY_QUERY", "CANCEL_PENDING", "PARTIALLY_FILLED"}:
                    await self._recover_uncertain(child, child_request)
                    continue
                # A durable pre-boundary row may resume through normal submit;
                # it will retain the same child identity and create no second POST.
                await self.submit(child_request)
                continue
            # Absence is safe only because the authoritative parent manifest is
            # durable and no child row has crossed a mutation boundary.
            self.orders.create({
                **child_request.__dict__, "client_order_id": deterministic_client_order_id(child_request.intent_id),
                "state": "INTENT_CREATED",
            })
            await self.submit(child_request)

        children = [self.orders.get(str(entry["intent_id"])) or {} for entry in entries]
        pending = [row for row in children if not row.get("terminal_finalized_at")]
        if pending:
            self.orders.transition(
                request.intent_id, "INTENT_CREATED", recovery_required=True,
                recovery_reason="staged_children_recovery_pending",
            )
            return self._result_from_row(self.orders.get(request.intent_id) or parent, "staged_children_recovery_pending", ok=False)
        filled = sum(float(row.get("filled_contracts", 0.0) or 0.0) for row in children)
        child_results = [
            {**self._result_from_row(row, "staged_child_terminal").__dict__,
             "terminal_class": str(row.get("terminal_class", "") or ""),
             "terminal_reason": str(row.get("terminal_reason", "") or "")}
            for row in children
        ]
        if filled <= 0:
            row = await self._finalize_terminal(
                request, state="FAILED_FINAL", terminal_class="reconciliation_terminal",
                reason="staged_no_fill", exchange_contacted=any(bool(row.get("exchange_order_id")) for row in children),
                fields={"child_results": child_results},
            )
            return self._result_from_row(row, "staged_no_fill", ok=False)
        weighted = sum(float(row.get("filled_contracts", 0.0) or 0.0) * float(row.get("average_fill_price", 0.0) or 0.0) for row in children)
        instrument = self.instruments.require(request.coin)
        state = "FILLED" if abs(filled - request.approved_contracts) <= max(instrument.lot_sz / 2, 1e-9) else "PARTIALLY_FILLED"
        child_classes = [str(row.get("terminal_class", "") or "") for row in children]
        # Aggregate quantity cannot erase a terminal child failure.  Keep the
        # aggregate fill durable, but select the most safety-relevant child
        # taxonomy for the parent result so callers never treat an unprotected
        # or rejected staged mutation as success.
        if "protection_failure" in child_classes:
            row = await self._finalize_terminal(
                request, state="FAILED_FINAL", terminal_class="protection_failure",
                reason="staged_child_protection_failure", exchange_contacted=True,
                fields={"filled_contracts": filled, "average_fill_price": weighted / filled,
                        "child_results": child_results, "staged_completion_state": state,
                        "terminal_family": "staged_child_failure"},
            )
            return self._result_from_row(row, "staged_child_protection_failure", ok=False)
        failure_classes = [
            value for value in child_classes
            if value and value not in {"filled"}
        ]
        if failure_classes:
            priority = ("exchange_rejection", "local_rejection", "reconciliation_terminal", "cancelled")
            terminal_class = next((value for value in priority if value in failure_classes), failure_classes[0])
            row = await self._finalize_terminal(
                request, state="FAILED_FINAL", terminal_class=terminal_class,
                reason="staged_child_terminal_failure", exchange_contacted=True,
                fields={"filled_contracts": filled, "average_fill_price": weighted / filled,
                        "child_results": child_results, "staged_completion_state": state,
                        "terminal_family": "staged_child_failure"},
            )
            return self._result_from_row(row, "staged_child_terminal_failure", ok=False)
        if state != "FILLED":
            # A terminal child result is not proof that the parent obtained
            # its requested aggregate. Keep the parent reservation and child
            # evidence recoverable until an authoritative full-fill proof (or
            # explicit terminal failure) resolves the underfill.
            row = self.orders.transition(
                request.intent_id,
                "PARTIALLY_FILLED",
                filled_contracts=filled,
                average_fill_price=weighted / filled,
                child_results=child_results,
                staged_completion_state=state,
                staged_children_terminal=True,
                staged_underfill=True,
                staged_recovery_action="retain_parent_reservation_no_repost",
                recovery_required=True,
                recovery_reason="staged_underfill_requires_authoritative_full_fill",
            )
            return self._result_from_row(row, "staged_underfill_requires_authoritative_full_fill", ok=False)
        row = await self._finalize_terminal(
            request, state="FILLED", terminal_class="filled",
            reason="staged_executed", exchange_contacted=True,
            fields={"filled_contracts": filled, "average_fill_price": weighted / filled,
                    "child_results": child_results,
                    "staged_completion_state": state},
        )
        return self._result_from_row(row, "staged_executed", ok=True)

    async def _submit_once(self, request: ExecutionRequest, client_order_id: str, on_exchange_request) -> dict | None:
        side = "buy" if request.direction.lower() in {"long", "buy"} else "sell"
        if request.order_type == "limit":
            return await self.client.place_limit_order(
                request.coin, side, request.price, request.approved_notional_usdt,
                sl_price=request.stop_price, tp_price=request.take_profit,
                leverage=request.leverage, client_order_id=client_order_id,
                contracts_override=request.approved_contracts,
                position_mode=str(request.metadata.get("position_mode", "") or ""),
                on_exchange_request=on_exchange_request,
            )
        return await self.client.place_market_order(
            request.coin, side, request.approved_notional_usdt,
            sl_price=request.stop_price, tp_price=request.take_profit,
            leverage=request.leverage, client_order_id=client_order_id,
            contracts_override=request.approved_contracts,
            position_mode=str(request.metadata.get("position_mode", "") or ""),
            allow_nano_route=False,
            on_exchange_request=on_exchange_request,
        )

    async def _confirm_live_state(self, request: ExecutionRequest, order_id: str, client_order_id: str) -> dict:
        row = self.orders.transition(request.intent_id, "ACKNOWLEDGED")
        protected_qty = 0.0
        deadline = time.monotonic() + max(request.timeout_secs, 0.1)
        while time.monotonic() < deadline:
            status = await self.client.get_order(request.coin, order_id=order_id, client_order_id=client_order_id)
            if status:
                state = str(status.get("state", "")).lower()
                fill_evidence, cumulative_fill = _authoritative_cumulative_fill(status)
                avg_px = float(status.get("avgPx", status.get("fillPx", 0.0)) or 0.0)
                if fill_evidence == FILL_UNKNOWN:
                    reason = (
                        "filled_cumulative_evidence_unknown"
                        if state == "filled"
                        else "partial_fill_cumulative_evidence_unknown"
                    )
                    return self._mark_recovery_pending(
                        request,
                        state="SUBMIT_UNCERTAIN" if state == "filled" else "PARTIALLY_FILLED",
                        reason=reason,
                        exchange_order_id=order_id,
                        order_status=status,
                        cumulative_fill_evidence=FILL_UNKNOWN,
                        recovery_evidence={"order": status, "recovery_action": "await_authoritative_cumulative_fill"},
                    )
                filled = float(cumulative_fill or 0.0)
                if state == "filled" and fill_evidence != FILL_KNOWN_POSITIVE:
                    return self._mark_recovery_pending(
                        request, state="SUBMIT_UNCERTAIN",
                        reason="filled_cumulative_fill_not_positive",
                        exchange_order_id=order_id, order_status=status,
                        cumulative_fill_evidence=fill_evidence,
                        recovery_evidence={"order": status, "recovery_action": "await_positive_cumulative_fill"},
                    )
                previous_cumulative = float(
                    row.get("authoritative_cumulative_fill", row.get("filled_contracts", 0.0)) or 0.0
                )
                fill_delta = max(filled - previous_cumulative, 0.0)
                if fill_delta > 0:
                    self.orders.add_fill(
                        request.intent_id,
                        {
                            "fillId": status.get("tradeId", f"ORD-{order_id}-CUM-{filled}"),
                            "fillSz": fill_delta,
                            "fillPx": avg_px,
                            "fee": status.get("fee", 0),
                        },
                    )
                if state == "filled":
                    # Exchange fill truth is authoritative, but lifecycle
                    # completion waits for required protection/readback.
                    return self.orders.transition(
                        request.intent_id, "PARTIALLY_FILLED",
                        filled_contracts=filled, average_fill_price=avg_px,
                        exchange_order_id=order_id, order_status=status,
                        exchange_fill_confirmed=True,
                        authoritative_cumulative_fill=filled,
                        cumulative_fill_evidence=fill_evidence,
                    )
                if state == "partially_filled":
                    if fill_evidence == FILL_UNKNOWN:
                        return self.orders.transition(
                            request.intent_id, "PARTIALLY_FILLED", order_status=status,
                            recovery_required=True,
                            recovery_reason="partial_fill_cumulative_evidence_unknown",
                            cumulative_fill_evidence=FILL_UNKNOWN,
                        )
                    row = self.orders.transition(
                        request.intent_id, "PARTIALLY_FILLED",
                        filled_contracts=filled, average_fill_price=avg_px,
                        authoritative_cumulative_fill=filled,
                        cumulative_fill_evidence=fill_evidence,
                    )
                    if filled > protected_qty + 1e-12:
                        try:
                            protection = await self.protection.ensure(
                                coin=request.coin,
                                direction=request.direction,
                                contracts=filled,
                                stop_price=request.stop_price,
                                take_profit=request.take_profit,
                                client_order_id=client_order_id,
                                paper=False,
                                tp_owner=str(request.metadata.get("tp_owner", "") or ""),
                            )
                        except Exception as exc:
                            # The live remainder cannot be terminalized from a
                            # failed local protection attempt.  Keep all
                            # reservation and restart reconciliation ownership
                            # on this durable partial intent.
                            return self._mark_recovery_pending(
                                request, state="PARTIALLY_FILLED",
                                reason="live_partial_protection_failure",
                                filled_contracts=filled, average_fill_price=avg_px,
                                exchange_order_id=order_id, order_status=status,
                                protection={"protected": False, "reason": f"{type(exc).__name__}:{exc}"},
                                protection_failure=True,
                                recovery_evidence={"order": status, "protection_error": f"{type(exc).__name__}:{exc}"},
                            )
                        row = self.orders.transition(request.intent_id, "PARTIALLY_FILLED", protection=protection.__dict__)
                        if not protection.protected:
                            # A live remainder remains exchange-owned.  Keep
                            # durable recovery ownership even though the
                            # currently-filled exposure is explicitly
                            # unprotected; terminal truth must wait for the
                            # order remainder's authoritative terminal state.
                            return self.orders.transition(
                                request.intent_id, "PARTIALLY_FILLED",
                                filled_contracts=filled, average_fill_price=avg_px,
                                protection=protection.__dict__, order_status=status,
                                protection_failure=True,
                                recovery_required=True,
                                recovery_reason="live_partial_protection_failure",
                            )
                        protected_qty = filled
                if state in {"canceled", "cancelled", "expired", "mmp_canceled"}:
                    if fill_evidence == FILL_UNKNOWN:
                        return self.orders.transition(
                            request.intent_id, "CANCEL_PENDING", order_status=status,
                            recovery_required=True,
                            recovery_reason="terminal_cancel_cumulative_evidence_unknown",
                            cumulative_fill_evidence=FILL_UNKNOWN,
                        )
                    requested = float(request.requested_contracts or request.approved_contracts or 0.0)
                    if 0.0 < filled < requested:
                        return await self._converge_authoritative_partial_cancel(
                            request, status, {}, order_id,
                        )
                    if fill_evidence == FILL_KNOWN_ZERO and not self._prepare_zero_fill_risk_slot(request.intent_id):
                        return self.orders.transition(
                            request.intent_id, "CANCEL_PENDING", order_status=status,
                            recovery_required=True, recovery_reason="authoritative_cancel_risk_slot_close_persistence_pending",
                        )
                    final = await self._finalize_terminal(
                        request, state="CANCELLED", terminal_class="cancelled", reason="authoritative_order_cancelled",
                        exchange_contacted=True,
                        fields={"filled_contracts": filled, "average_fill_price": avg_px,
                                "exchange_order_id": order_id, "order_status": status,
                                "cumulative_fill_evidence": fill_evidence},
                    )
                    if fill_evidence == FILL_KNOWN_ZERO:
                        self._reconcile_authoritative_zero_fill_risk_slot(request.intent_id)
                    return final
            await asyncio.sleep(0.25)
        if row.get("state") == "PARTIALLY_FILLED":
            # This is a physical exchange mutation.  Persist the cancellation
            # boundary before the first request so restart recovery only reads
            # back the remainder and can never send a duplicate cancel/repost.
            row = self.orders.transition(
                request.intent_id, "CANCEL_PENDING",
                filled_contracts=float(row.get("filled_contracts", 0.0) or 0.0),
                average_fill_price=float(row.get("average_fill_price", 0.0) or 0.0),
                remainder_cancel_requested=True, cancel_attempted=True,
                cancel_physical_attempt_count=1,
                cancel_mutation_boundary_at=time.time(),
                recovery_required=True,
                recovery_reason="partial_remainder_cancel_confirmation_required",
            )
            try:
                cancel_response = await self.client.cancel_order(
                    f"{request.coin.upper()}-USDT-SWAP", order_id,
                    client_order_id=client_order_id,
                )
            except Exception as exc:
                return self.orders.transition(
                    request.intent_id, "CANCEL_PENDING",
                    cancel_error=f"{type(exc).__name__}:{exc}",
                    recovery_required=True,
                    recovery_reason="partial_remainder_cancel_readback_required",
                )
            return self.orders.transition(
                request.intent_id, "CANCEL_PENDING",
                filled_contracts=float(row.get("filled_contracts", 0.0) or 0.0),
                average_fill_price=float(row.get("average_fill_price", 0.0) or 0.0),
                remainder_cancel_requested=True, cancel_response=cancel_response,
                recovery_required=True, recovery_reason="partial_remainder_cancel_confirmation_required",
            )
        return self.orders.transition(request.intent_id, "SUBMIT_UNCERTAIN", submit_error="order_status_timeout")

    def _fallback_request(self, parent_request: ExecutionRequest, child_intent_id: str) -> ExecutionRequest:
        return ExecutionRequest(**{
            **parent_request.__dict__, "intent_id": child_intent_id,
            "order_type": "market", "fallback_mode": "market",
            "metadata": {**dict(parent_request.metadata or {}),
                         "fallback_parent_intent_id": parent_request.intent_id,
                         "reservation_owner_intent_id": parent_request.intent_id},
        })

    def _establish_fallback_child(self, parent_request: ExecutionRequest, parent: dict) -> tuple[ExecutionRequest | None, str]:
        """Create then link a fallback child; neither step may imply a POST."""
        plan = dict(parent.get("fallback_plan", {}) or {})
        child_intent_id = str(plan.get("child_intent_id", parent.get("fallback_child_intent_id", "")) or "")
        if not child_intent_id or str(plan.get("parent_intent_id", parent_request.intent_id) or "") != parent_request.intent_id:
            return None, "fallback_plan_missing_or_invalid"
        child_request = self._fallback_request(parent_request, child_intent_id)
        child = self.orders.get(child_intent_id)
        if not child:
            try:
                self.orders.create({
                    **child_request.__dict__, "client_order_id": deterministic_client_order_id(child_intent_id),
                    "state": "INTENT_CREATED",
                })
            except Exception as exc:
                self.orders.transition(
                    parent_request.intent_id, "CANCEL_PENDING",
                    fallback_establishment_status="pending",
                    fallback_establishment_error=f"child_persistence_failed:{type(exc).__name__}:{exc}",
                    recovery_required=True, recovery_reason="fallback_child_establishment_pending",
                )
                return None, "fallback_child_establishment_pending"
        # A crash/failure here leaves an existing pre-boundary child. Recovery
        # repeats only this local linkage write before the child may submit.
        try:
            self.orders.transition(
                parent_request.intent_id, "CANCEL_PENDING",
                fallback_child_intent_id=child_intent_id,
                fallback_establishment_status="established",
                fallback_establishment_error="",
                recovery_required=True, recovery_reason="fallback_child_established",
            )
        except Exception:
            return None, "fallback_child_linkage_pending"
        return child_request, "fallback_child_established"

    async def _limit_timeout_fallback(self, request: ExecutionRequest, order_id: str, client_order_id: str) -> dict:
        status = await self.client.get_order(request.coin, order_id=order_id, client_order_id=client_order_id)
        if not status:
            return self.orders.transition(request.intent_id, "SUBMIT_UNCERTAIN", submit_error="fallback_blocked_order_unknown")
        state = str(status.get("state", "")).lower()
        confirmed = status if state in {"canceled", "cancelled", "expired", "mmp_canceled"} else None
        if state in {"live", "partially_filled"}:
            fill_evidence, live_fill = _authoritative_cumulative_fill(status)
            if fill_evidence == FILL_KNOWN_POSITIVE:
                return self.orders.transition(request.intent_id, "PARTIALLY_FILLED", fallback_blocked="partial_fill_exists")
            # Persist the fallback cancellation boundary before the physical
            # cancel.  A process loss after this row is recovery-owned and
            # read-only; it may query confirmation but must not cancel again.
            self.orders.transition(
                request.intent_id, "CANCEL_PENDING",
                cancel_attempted=True,
                cancel_physical_attempt_count=1,
                cancel_mutation_boundary_at=time.time(),
                recovery_required=True,
                recovery_reason="fallback_cancel_confirmation_required",
            )
            try:
                await self.client.cancel_order(
                    f"{request.coin.upper()}-USDT-SWAP", order_id,
                    client_order_id=client_order_id,
                )
            except Exception as exc:
                return self.orders.transition(
                    request.intent_id, "CANCEL_PENDING",
                    cancel_error=f"{type(exc).__name__}:{exc}",
                    recovery_required=True,
                    recovery_reason="fallback_cancel_readback_required",
                )
            confirmed = await self.client.get_order(request.coin, order_id=order_id, client_order_id=client_order_id)
            if not isinstance(confirmed, dict) or str(confirmed.get("state", "")).lower() not in {"canceled", "cancelled", "expired", "mmp_canceled"}:
                return self.orders.transition(
                    request.intent_id, "CANCEL_PENDING",
                    cancel_confirmation=dict(confirmed or {}),
                    recovery_required=True,
                    recovery_reason="fallback_cancel_confirmation_required",
                )
        elif state not in {"canceled", "cancelled", "expired", "mmp_canceled"}:
            return self.orders.transition(request.intent_id, "SUBMIT_UNCERTAIN", submit_error=f"fallback_blocked_state_{state}")
        fill_evidence, confirmed_fill = _authoritative_cumulative_fill(confirmed)
        if fill_evidence == FILL_UNKNOWN:
            return self.orders.transition(
                request.intent_id, "CANCEL_PENDING", cancel_confirmation=confirmed,
                recovery_required=True, recovery_reason="fallback_cumulative_fill_evidence_unknown",
                cumulative_fill_evidence=FILL_UNKNOWN,
            )
        if fill_evidence == FILL_KNOWN_POSITIVE:
            # A surviving partial position owns the original capacity until it
            # actually closes.  It is never eligible for a zero-fill fallback.
            return self.orders.transition(
                request.intent_id, "PARTIALLY_FILLED", filled_contracts=float(confirmed_fill or 0.0),
                average_fill_price=float((confirmed or {}).get("avgPx", 0.0) or 0.0),
                fallback_blocked="partial_fill_exists", cancel_confirmation=confirmed,
            )
        # Persist authoritative cancellation evidence before the original
        # slot's close operation.  The parent remains nonterminal only because
        # its reservation is about to be owned by the durable fallback child.
        self.orders.transition(
            request.intent_id, "CANCEL_PENDING", cancel_confirmation=confirmed,
            authoritative_cancellation=True, filled_contracts=0.0,
            cumulative_fill_evidence=FILL_KNOWN_ZERO,
            recovery_required=True, recovery_reason="fallback_risk_slot_close_pending",
        )
        fallback_intent_id = f"{request.intent_id}-F"
        # Cancellation is authoritative.  Persist the continuation plan before
        # releasing the original policy slot: a crash at either side of that
        # release now has durable ownership to resume, while capital remains
        # parent-owned until the child terminalizes.
        self.orders.transition(
            request.intent_id, "CANCEL_PENDING",
            fallback_started=True,
            fallback_reservation_owner_intent_id=request.intent_id,
            fallback_plan={"version": 1, "parent_intent_id": request.intent_id,
                           "child_intent_id": fallback_intent_id,
                           "reservation_owner_intent_id": request.intent_id,
                           "reservation_id": request.reservation_id},
            fallback_establishment_status="WAITING_FOR_ORIGINAL_RISK_RELEASE",
            cancel_confirmation=confirmed,
            recovery_required=True,
            recovery_reason="fallback_original_risk_release_pending",
        )
        # The original zero-fill slot must be durably/safely released before
        # even creating the fallback child; reservation ownership is separate.
        if not self._reconcile_authoritative_zero_fill_risk_slot(request.intent_id, authoritative=True):
            return self.orders.transition(
                request.intent_id, "CANCEL_PENDING", cancel_confirmation=confirmed,
                recovery_required=True, recovery_reason="fallback_blocked_risk_slot_close_pending",
            )
        fallback, establishment = self._establish_fallback_child(request, self.orders.get(request.intent_id) or {})
        if fallback is None:
            return self.orders.get(request.intent_id) or {}
        result = await self.submit(fallback)
        fallback_row = self.orders.get(fallback_intent_id) or {}
        if fallback_row.get("terminal_finalized_at"):
            await self._finalize_terminal(
                request, state="CANCELLED", terminal_class="cancelled",
                reason="authoritative_cancel_before_fallback_resolved", exchange_contacted=True,
                fields={"fallback_started": True, "fallback_child_intent_id": fallback_intent_id,
                        "fallback_terminal_state": fallback_row.get("state"),
                        "cancel_confirmation": confirmed},
            )
        return self.orders.get(request.intent_id) or {}

    async def _recover_uncertain(self, existing: dict, request: ExecutionRequest) -> ExecutionResult:
        """Resolve an uncertain POST using read-only exchange evidence only."""
        self.orders.transition(request.intent_id, "RECOVERY_QUERY")
        client_order_id = str(existing.get("client_order_id", "") or "")
        inst_id = f"{request.coin.upper()}-USDT-SWAP"
        errors: list[str] = []

        async def read(label: str, awaitable, default):
            try:
                return await awaitable
            except Exception as exc:
                errors.append(f"{label}:{type(exc).__name__}:{exc}")
                return default

        status_reader = getattr(self.client, "get_order_status", None)
        status_probe = await read(
            "order_detail",
            status_reader(
                request.coin,
                order_id=str(existing.get("exchange_order_id", "") or ""),
                client_order_id=client_order_id,
            )
            if callable(status_reader)
            else self.client.get_order(
                request.coin,
                order_id=str(existing.get("exchange_order_id", "") or ""),
                client_order_id=client_order_id,
            ),
            {},
        )
        if isinstance(status_probe, dict) and "ok" in status_probe and not bool(status_probe.get("ok")):
            errors.append(f"order_detail:{status_probe.get('reason', 'order_detail_failed')}")
        status = status_probe.get("order", {}) if isinstance(status_probe, dict) and "order" in status_probe else status_probe
        order_detail_evidence = {
            "classification": str(
                status_probe.get("classification", "")
                if isinstance(status_probe, dict)
                else ""
            ),
            "found": bool(status_probe.get("found"))
            if isinstance(status_probe, dict) and "found" in status_probe
            else bool(status),
            "reason": str(status_probe.get("reason", "") if isinstance(status_probe, dict) else ""),
            "exchange_code": str(status_probe.get("exchange_code", "") if isinstance(status_probe, dict) else ""),
        }
        history_status_reader = getattr(self.client, "get_order_history_status", None)
        history_reader = getattr(self.client, "get_order_history", None)
        request_reader = getattr(self.client, "_request", None)
        history = await read(
            "order_history",
            history_status_reader(inst_id=inst_id, limit=100, priority="P0")
            if callable(history_status_reader)
            else history_reader(inst_id=inst_id, limit=100, priority="P0")
            if callable(history_reader)
            else request_reader(
                "GET", "/api/v5/trade/orders-history", params={"instType": "SWAP", "instId": inst_id, "limit": "100"}
            )
            if callable(request_reader)
            else asyncio.sleep(0, result=[]),
            [],
        )
        if isinstance(history, dict):
            if "ok" in history and not bool(history.get("ok")):
                errors.append(f"order_history:{history.get('reason', 'order_history_failed')}")
            history = list(history.get("orders", history.get("data", [])) or [])
        fills_status_reader = getattr(self.client, "get_fills_status", None)
        fills_reader = getattr(self.client, "get_fills", None)
        fills = await read(
            "fills",
            fills_status_reader(inst_id=inst_id, limit=100, priority="P0")
            if callable(fills_status_reader)
            else fills_reader(inst_id=inst_id, limit=100, priority="P0")
            if callable(fills_reader)
            else asyncio.sleep(0, result=[]),
            [],
        )
        if isinstance(fills, dict):
            if "ok" in fills and not bool(fills.get("ok")):
                errors.append(f"fills:{fills.get('reason', 'fills_failed')}")
            fills = list(fills.get("fills", []) or [])
        positions_status_reader = getattr(self.client, "get_positions_status", None)
        positions_reader = getattr(self.client, "get_positions", None)
        positions_status = await read(
            "positions",
            positions_status_reader()
            if callable(positions_status_reader)
            else positions_reader()
            if callable(positions_reader)
            else asyncio.sleep(0, result=[]),
            {},
        )
        if isinstance(positions_status, dict) and not bool(positions_status.get("ok", False)):
            errors.append(f"positions:{positions_status.get('reason', 'positions_failed')}")
        positions = (
            list(positions_status.get("positions", []) or [])
            if isinstance(positions_status, dict)
            else list(positions_status or [])
        )

        cancellation_remainder_outstanding = bool(
            str(existing.get("state", "") or "") == "CANCEL_PENDING"
            or existing.get("remainder_cancel_requested")
        ) and float(existing.get("filled_contracts", 0.0) or 0.0) < float(request.requested_contracts or request.approved_contracts or 0.0)
        if errors:
            if cancellation_remainder_outstanding:
                row = self._mark_recovery_pending(
                    request, state="CANCEL_PENDING",
                    reason="partial_remainder_cancel_query_unresolved",
                    filled_contracts=float(existing.get("filled_contracts", 0.0) or 0.0),
                    average_fill_price=float(existing.get("average_fill_price", 0.0) or 0.0),
                    recovery_evidence={"order_detail": order_detail_evidence, "query_errors": errors},
                )
                return self._result_from_row(row, "partial_remainder_cancel_query_unresolved", ok=False)
            row = self._mark_recovery_pending(
                request, state="SUBMIT_UNCERTAIN",
                reason="recovery_query_failed:" + ";".join(errors),
                submit_error="recovery_query_failed:" + ";".join(errors),
                recovery_evidence={"order_detail": order_detail_evidence, "query_errors": errors},
            )
            return self._result_from_row(row, "recovery_query_failed", ok=False)

        history_rows = [
            row for row in history
            if str(row.get("clOrdId", row.get("client_order_id", "")) or "") == client_order_id
        ]
        order = dict(status or {}) or (max(history_rows, key=lambda row: int(row.get("uTime", 0) or 0)) if history_rows else {})
        order_id = str(order.get("ordId", existing.get("exchange_order_id", "")) or "")
        order_state = str(order.get("state", "")).lower()
        def _fill_matches(fill: dict) -> bool:
            fill_order_id = str(fill.get("ordId", "") or "")
            fill_client_order_id = str(fill.get("clOrdId", "") or "")
            # Never accept an uncorrelated fill as evidence for an uncertain
            # submission.  At least one exchange/client identifier must match.
            return bool(
                (order_id and fill_order_id == order_id)
                or (client_order_id and fill_client_order_id == client_order_id)
            )

        matching_fills = [fill for fill in fills if _fill_matches(fill)]
        live_positions = [
            pos for pos in positions
            if (
                str(pos.get("instId", "")) == inst_id
                and abs(float(pos.get("pos", 0.0) or 0.0)) > 0
                and str(pos.get("posSide", "") or "").lower()
                in {str(request.direction or "").lower(), "", "net"}
            )
        ]

        correlated_fill_qty = sum(
            float(fill.get("fillSz", fill.get("qty", 0.0)) or 0.0)
            for fill in matching_fills
        )
        cumulative_evidence, cumulative_fill = _authoritative_cumulative_fill(order)
        fill_evidence = cumulative_evidence
        # Correlated event fills and same-symbol positions establish that
        # exposure *may* exist, but they cannot supply this order's cumulative
        # quantity.  Only ``accFillSz`` from the correlated order may drive a
        # recovered FILLED or live-partial lifecycle.
        fill_qty = float(cumulative_fill or 0.0) if cumulative_evidence != FILL_UNKNOWN else correlated_fill_qty
        cancellation_remainder_outstanding = cancellation_remainder_outstanding or (
            bool(existing.get("remainder_cancel_requested"))
            and fill_qty < float(request.requested_contracts or request.approved_contracts or 0.0)
        )
        avg_px = 0.0
        if matching_fills and fill_qty > 0:
            avg_px = sum(
                float(fill.get("fillPx", fill.get("price", 0.0)) or 0.0)
                * float(fill.get("fillSz", fill.get("qty", 0.0)) or 0.0)
                for fill in matching_fills
            ) / fill_qty
        if avg_px <= 0:
            avg_px = float(order.get("avgPx", order.get("fillPx", 0.0)) or 0.0)
        if avg_px <= 0 and live_positions:
            avg_px = float(live_positions[0].get("avgPx", live_positions[0].get("markPx", 0.0)) or 0.0)

        requested_qty = float(request.requested_contracts or request.approved_contracts or 0.0)
        # A terminal exchange ``filled`` state must carry its own positive
        # cumulative quantity. Correlated fills or a current position can show
        # that exposure may exist, but cannot replace the authoritative order
        # cumulative evidence required to classify this intent as FILLED.
        if order_state == "filled" and cumulative_evidence != FILL_KNOWN_POSITIVE:
            row = self._mark_recovery_pending(
                request,
                state="SUBMIT_UNCERTAIN",
                reason="recovered_filled_cumulative_evidence_unknown",
                exchange_order_id=order_id,
                filled_contracts=float(existing.get("filled_contracts", 0.0) or 0.0),
                average_fill_price=float(existing.get("average_fill_price", 0.0) or 0.0),
                cumulative_fill_evidence=cumulative_evidence,
                recovery_evidence={
                    "order": order,
                    "fills": matching_fills,
                    "positions": live_positions,
                    "recovery_action": "await_authoritative_cumulative_fill",
                },
            )
            return self._result_from_row(row, "recovered_filled_cumulative_evidence_unknown", ok=False)
        if (
            (correlated_fill_qty > 0 or live_positions)
            and cumulative_evidence != FILL_KNOWN_POSITIVE
        ):
            evidence_reason = (
                "same_symbol_position_without_authoritative_order_cumulative_fill"
                if live_positions and correlated_fill_qty <= 0
                else "correlated_fill_without_authoritative_order_cumulative_fill"
            )
            row = self._mark_recovery_pending(
                request,
                state="SUBMIT_UNCERTAIN",
                reason=evidence_reason,
                exchange_order_id=order_id,
                filled_contracts=float(existing.get("filled_contracts", 0.0) or 0.0),
                average_fill_price=float(existing.get("average_fill_price", 0.0) or 0.0),
                cumulative_fill_evidence=cumulative_evidence,
                recovery_evidence={
                    "order": order,
                    "fills": matching_fills,
                    "positions": live_positions,
                    "recovery_action": "await_authoritative_order_cumulative_fill",
                },
            )
            return self._result_from_row(row, evidence_reason, ok=False)
        prior_partial_protection_reason = str(existing.get("recovery_reason", "") or "")
        # A prior live partial-protection outcome is durable recovery truth.
        # While the authoritative order snapshot still proves that its
        # unfilled remainder is live, recovery is deliberately read-only: do
        # not retry protection, update the position registry, terminalize, or
        # release the parent reservation.  A later successful local protection
        # call cannot make a still-partially-filled exchange order terminal.
        if (
            str(existing.get("state", "") or "") == "PARTIALLY_FILLED"
            and prior_partial_protection_reason in {
                "live_partial_protection_failure",
                "recovered_live_partial_protection_confirmed",
            }
            and order_state == "partially_filled"
            and fill_evidence == FILL_KNOWN_POSITIVE
            and cumulative_fill is not None
            and 0.0 < cumulative_fill < requested_qty
        ):
            row = self._mark_recovery_pending(
                request,
                state="PARTIALLY_FILLED",
                reason=prior_partial_protection_reason,
                filled_contracts=float(cumulative_fill),
                average_fill_price=avg_px,
                exchange_order_id=order_id,
                order_status=order,
                protection_failure=(prior_partial_protection_reason == "live_partial_protection_failure"),
                cumulative_fill_evidence=fill_evidence,
                recovery_evidence={
                    "order": order,
                    "fills": matching_fills,
                    "positions": live_positions,
                    "recovery_action": "live_partial_protection_retained",
                },
            )
            return self._result_from_row(row, prior_partial_protection_reason, ok=False)

        # A live partial is not a completed entry.  Its authoritative order
        # quantity, rather than successful local protection, controls the
        # lifecycle transition.  Protect the filled exposure once if needed,
        # then retain the intent and reservation for later exchange-terminal
        # convergence.  In particular, do not let the generic recovered-fill
        # path terminalize this row as FILLED and release its risk capacity.
        live_partial = bool(
            order_state == "partially_filled"
            and fill_evidence == FILL_KNOWN_POSITIVE
            and cumulative_fill is not None
            and 0.0 < cumulative_fill < requested_qty
        )
        if live_partial:
            try:
                protection = await self.protection.ensure(
                    coin=request.coin,
                    direction=request.direction,
                    contracts=float(cumulative_fill),
                    stop_price=request.stop_price,
                    take_profit=request.take_profit,
                    client_order_id=client_order_id,
                    paper=False,
                    tp_owner=str(request.metadata.get("tp_owner", "") or ""),
                )
            except Exception as exc:
                row = self._mark_recovery_pending(
                    request, state="PARTIALLY_FILLED",
                    reason="live_partial_protection_failure",
                    filled_contracts=float(cumulative_fill), average_fill_price=avg_px,
                    exchange_order_id=order_id, protection_failure=True,
                    cumulative_fill_evidence=fill_evidence,
                    recovery_evidence={
                        "order": order, "fills": matching_fills, "positions": live_positions,
                        "followup_error": f"{type(exc).__name__}:{exc}",
                    },
                )
                return self._result_from_row(row, "live_partial_protection_failure", ok=False)
            if not protection.protected:
                row = self._mark_recovery_pending(
                    request, state="PARTIALLY_FILLED",
                    reason="live_partial_protection_failure",
                    filled_contracts=float(cumulative_fill), average_fill_price=avg_px,
                    exchange_order_id=order_id, protection=protection.__dict__,
                    protection_failure=True, cumulative_fill_evidence=fill_evidence,
                    recovery_evidence={"order": order, "fills": matching_fills,
                                       "positions": live_positions},
                )
                return self._result_from_row(row, "live_partial_protection_failure", ok=False)
            row = self._mark_recovery_pending(
                request, state="PARTIALLY_FILLED",
                reason="recovered_live_partial_protection_confirmed",
                filled_contracts=float(cumulative_fill), average_fill_price=avg_px,
                exchange_order_id=order_id, protection=protection.__dict__,
                cumulative_fill_evidence=fill_evidence,
                recovery_evidence={"order": order, "fills": matching_fills,
                                   "positions": live_positions,
                                   "recovery_action": "live_partial_protected_retained"},
            )
            return self._result_from_row(row, "recovered_live_partial_protection_confirmed", ok=False)

        if fill_qty > 0 or live_positions:
            effective = self._effective_position_leverage_from_rows(
                live_positions, inst_id, request.direction,
            )
            if not effective:
                row = self._mark_recovery_pending(
                    request,
                    state="SUBMIT_UNCERTAIN",
                    reason="recovered_fill_effective_leverage_unknown",
                    exchange_order_id=order_id,
                    filled_contracts=float(existing.get("filled_contracts", 0.0) or 0.0),
                    average_fill_price=float(existing.get("average_fill_price", 0.0) or 0.0),
                    cumulative_fill_evidence=fill_evidence,
                    recovery_evidence={
                        "order": order, "fills": matching_fills,
                        "positions": live_positions,
                        "recovery_action": "await_effective_position_leverage",
                    },
                )
                return self._result_from_row(row, "recovered_fill_effective_leverage_unknown", ok=False)
            lifecycle_leverage = int(effective)
            leverage_status = "POSITION_EFFECTIVE" if effective else "LEVERAGE_UNKNOWN"
            leverage_meta = {
                "requested_leverage": request.requested_leverage or request.leverage,
                "exchange_configured_leverage": request.exchange_configured_leverage or request.leverage,
                "position_effective_leverage": effective,
                "leverage_truth_status": leverage_status,
                "leverage_truth_timestamp": str(time.time()),
                "leverage_truth_source": "recovery.position" if effective else "recovery.position_missing",
                "leverage_truth_mismatch": bool(effective and effective != int(request.exchange_configured_leverage or request.leverage or effective)),
                "requested_vs_configured_mismatch": bool(
                    request.exchange_configured_leverage
                    and request.exchange_configured_leverage != request.requested_leverage
                ),
                "configured_vs_effective_mismatch": bool(
                    effective
                    and effective != int(request.exchange_configured_leverage or effective)
                ),
            }
            request = replace(
                request,
                leverage=lifecycle_leverage,
                position_effective_leverage=effective,
                leverage_truth_status=leverage_status,
                leverage_truth_timestamp=leverage_meta["leverage_truth_timestamp"],
                metadata={**dict(request.metadata or {}), **leverage_meta},
            )
            for fill in matching_fills:
                self.orders.add_fill(request.intent_id, dict(fill))
            # Preserve authoritative fill evidence in a recoverable state
            # while registry/protection side effects run.  Do not make it look
            # terminal until the canonical finalizer can atomically record the
            # terminal taxonomy and evidence.
            authoritative_partial_cancel = bool(
                fill_qty > 0
                and fill_qty < requested_qty
                and order_state in {"canceled", "cancelled", "expired", "mmp_canceled"}
            )
            partial_cancel_facts = {
                "requested_contracts": request.requested_contracts,
                "cancel_confirmation": order,
                "authoritative_cancellation": True,
                "partial_execution": True,
                "unfilled_remainder_cancelled": True,
            } if authoritative_partial_cancel else {}
            mapped = "PARTIALLY_FILLED"
            durable_before_recovery = self.orders.get(request.intent_id) or {}
            durable_open = dict(
                dict(durable_before_recovery.get("risk_slot_operations", {}) or {}).get("open", {}) or {}
            )
            needs_recovered_open_slot = (
                not bool(durable_before_recovery.get("risk_slot_acquired"))
                or str(durable_open.get("status", "") or "") != "complete"
            )
            row = self.orders.transition(
                request.intent_id,
                mapped,
                exchange_order_id=order_id,
                exchange_contacted=True,
                filled_contracts=fill_qty,
                average_fill_price=avg_px,
                requested_leverage=leverage_meta["requested_leverage"],
                exchange_configured_leverage=leverage_meta["exchange_configured_leverage"],
                position_effective_leverage=effective,
                leverage_truth_status=leverage_status,
                leverage_truth_timestamp=leverage_meta["leverage_truth_timestamp"],
                leverage=lifecycle_leverage,
                recovery_evidence={"order": order, "fills": matching_fills, "positions": live_positions},
                **partial_cancel_facts,
            )
            if needs_recovered_open_slot and not self._ensure_recovered_open_risk_slot(request.intent_id):
                # The exchange exposure is real, but capacity accounting is
                # not yet durably OPEN.  Do not run protection/registry
                # follow-up or terminalize it: retaining this recovery-owned
                # row is the only restart-safe way to retry the same
                # deterministic open operation.
                row = self._mark_recovery_pending(
                    request, state="PARTIALLY_FILLED",
                    reason="recovered_filled_risk_slot_open_pending",
                    exchange_order_id=order_id,
                    filled_contracts=fill_qty,
                    average_fill_price=avg_px,
                    recovery_evidence={"order": order, "fills": matching_fills,
                                       "positions": live_positions},
                )
                return self._result_from_row(row, "recovered_filled_risk_slot_open_pending", ok=False)
            if authoritative_partial_cancel:
                final = await self._converge_authoritative_partial_cancel(
                    request, order, {}, order_id,
                )
                return self._result_from_row(
                    final,
                    str(final.get("terminal_reason", "partial_execution_remainder_cancelled") or "partial_execution_remainder_cancelled"),
                    ok=str(final.get("terminal_class", "") or "") == "cancelled",
                )
            from position_registry import update as registry_update
            filled_notional = 0.0
            registry_activation_phase = True
            try:
                if authoritative_partial_cancel:
                    self._upsert_surviving_partial_cancel_position(
                        request, filled_contracts=fill_qty, average_fill_price=avg_px,
                        order_id=order_id, leverage=lifecycle_leverage,
                        leverage_metadata=leverage_meta, recovered=True,
                    )
                else:
                    instrument = self.instruments.require(request.coin)
                    if avg_px <= 0:
                        raise RuntimeError("recovered_authoritative_fill_price_unavailable")
                    filled_notional = instrument.contracts_to_notional(fill_qty, avg_px)
                    updated = registry_update(
                request.coin,
                size_usdt=filled_notional,
                entry=avg_px,
                leverage=lifecycle_leverage,
                stop_loss=float(request.stop_price),
                take_profit=float(request.take_profit),
                order_id=order_id,
                metadata={
                    "pending_reservation": False,
                    "entry_lifecycle": "FILLED",
                    "mutation_attempted": True,
                    "capital_reservation_id": request.reservation_id,
                    "execution_intent_id": request.intent_id,
                    "risk_slot_intent_id": self._risk_slot_owner_intent(request.intent_id),
                    "candidate_id": request.candidate_id,
                    **leverage_meta,
                    "exchange_confirmed": True,
                    "recovered_from_uncertain": True,
                    "desired_stop_price": float(request.stop_price or 0.0),
                    "exchange_confirmed_stop_price": 0.0,
                    "exchange_protection_status": "PROTECTION_INTENT_PENDING",
                    "tp_owner": str(request.metadata.get("tp_owner", "") or ""),
                    "tp_mode_at_entry": str(request.metadata.get("tp_mode_at_entry", "") or ""),
                    "local_tp1_enabled": bool(request.metadata.get("local_tp1_enabled", False)),
                    "local_tp2_enabled": bool(request.metadata.get("local_tp2_enabled", False)),
                        "tp_owner_timestamp": str(request.metadata.get("tp_owner_timestamp", request.metadata.get("ownership_timestamp", "")) or ""),
                        "partials_enabled_at_entry": bool(request.metadata.get("partials_enabled_at_entry", False)),
                        },
                        require_persist=True,
                    )
                    if not updated:
                        raise RuntimeError("durable_recovered_registry_activation_not_confirmed")
                registry_activation_phase = False
                protection = await self.protection.ensure(
                coin=request.coin,
                direction=request.direction,
                contracts=fill_qty,
                stop_price=request.stop_price,
                take_profit=request.take_profit,
                client_order_id=client_order_id,
                paper=False,
                tp_owner=str(request.metadata.get("tp_owner", "") or ""),
            )
                self.orders.transition(request.intent_id, mapped, protection=protection.__dict__)
            except Exception as exc:
                if registry_activation_phase or (
                    "registry_activation" in str(exc)
                    or "position_registry_missing_for_partial_cancel" in str(exc)
                ):
                    row = self._mark_recovery_pending(
                        request, state="PARTIALLY_FILLED",
                        reason="recovered_filled_registry_activation_pending",
                        filled_contracts=fill_qty,
                        average_fill_price=avg_px,
                        exchange_order_id=order_id,
                        registry_activation_error=f"{type(exc).__name__}:{exc}",
                        recovery_evidence={"order": order, "fills": matching_fills,
                                           "positions": live_positions},
                    )
                    return self._result_from_row(row, "recovered_filled_registry_activation_pending", ok=False)
                if (
                    order_state in {"live", "effective", "partially_filled"}
                    and 0.0 < fill_qty < requested_qty
                ):
                    try:
                        registry_update(
                            request.coin,
                            metadata={
                                "exchange_protection_status": "PROTECTION_FAILED",
                                "protection_failure": f"{type(exc).__name__}:{exc}",
                            },
                            require_persist=True,
                        )
                    except Exception:
                        logger.exception("Live partial protection-failure registry persistence pending for %s", request.intent_id)
                    row = self._mark_recovery_pending(
                        request, state="PARTIALLY_FILLED",
                        reason="live_partial_protection_failure",
                        filled_contracts=fill_qty, average_fill_price=avg_px,
                        exchange_order_id=order_id,
                        protection_failure=True,
                        recovery_evidence={"order": order, "fills": matching_fills,
                                           "positions": live_positions,
                                           "followup_error": f"{type(exc).__name__}:{exc}"},
                    )
                    return self._result_from_row(row, "live_partial_protection_failure", ok=False)
                final = await self._finalize_terminal(
                    request, state="CANCELLED" if authoritative_partial_cancel else "FILLED",
                    terminal_class="protection_failure",
                    reason="partial_cancel_protection_failure" if authoritative_partial_cancel else "recovered_fill_followup_failure", exchange_contacted=True,
                    fields={"filled_contracts": fill_qty, "average_fill_price": avg_px,
                            "exchange_order_id": order_id, **partial_cancel_facts,
                            "recovery_evidence": {"order": order, "fills": matching_fills,
                                                  "positions": live_positions,
                                                  "followup_error": f"{type(exc).__name__}:{exc}"}},
                )
                return self._result_from_row(final, "recovered_fill_followup_failure", ok=False)
            if not protection.protected:
                if (
                    order_state in {"live", "effective", "partially_filled"}
                    and 0.0 < fill_qty < requested_qty
                ):
                    try:
                        registry_update(
                            request.coin,
                            metadata={
                                "exchange_protection_status": str(
                                    protection.reason or protection.protection_state or "PROTECTION_FAILED"
                                ),
                                "protection_failure": True,
                            },
                            require_persist=True,
                        )
                    except Exception:
                        logger.exception("Live partial protection status persistence pending for %s", request.intent_id)
                    row = self._mark_recovery_pending(
                        request, state="PARTIALLY_FILLED",
                        reason="live_partial_protection_failure",
                        filled_contracts=fill_qty, average_fill_price=avg_px,
                        exchange_order_id=order_id, protection=protection.__dict__,
                        protection_failure=True,
                        recovery_evidence={"order": order, "fills": matching_fills,
                                           "positions": live_positions},
                    )
                    return self._result_from_row(row, "live_partial_protection_failure", ok=False)
                row = await self._finalize_terminal(
                    request, state="CANCELLED" if authoritative_partial_cancel else "FILLED",
                    terminal_class="protection_failure",
                    reason="partial_cancel_protection_failure" if authoritative_partial_cancel else "recovered_fill_unprotected", exchange_contacted=True,
                    fields={"rejection_reason": "partial_cancel_protection_failure" if authoritative_partial_cancel else "recovered_fill_unprotected", **partial_cancel_facts,
                            "recovery_evidence": {"protection": protection.__dict__},
                            "filled_contracts": fill_qty, "average_fill_price": avg_px,
                            "protection": protection.__dict__},
                )
                return self._result_from_row(row, "recovered_fill_unprotected", ok=False)
            if authoritative_partial_cancel:
                self._upsert_surviving_partial_cancel_position(
                    request, filled_contracts=fill_qty, average_fill_price=avg_px,
                    order_id=order_id, leverage=lifecycle_leverage,
                    leverage_metadata=leverage_meta, protection=protection,
                    recovered=True,
                )
            else:
                updated = registry_update(
                    request.coin,
                    size_usdt=filled_notional,
                    entry=avg_px,
                    leverage=lifecycle_leverage,
                    stop_loss=float(request.stop_price),
                    take_profit=float(request.take_profit),
                    order_id=order_id,
                    metadata={
                        "pending_reservation": False,
                        "entry_lifecycle": "FILLED",
                        "mutation_attempted": True,
                        "capital_reservation_id": request.reservation_id,
                        "execution_intent_id": request.intent_id,
                        "risk_slot_intent_id": self._risk_slot_owner_intent(request.intent_id),
                        "candidate_id": request.candidate_id,
                        **leverage_meta,
                        "exchange_confirmed": True,
                        "recovered_from_uncertain": True,
                        "desired_stop_price": float(request.stop_price or 0.0),
                        "local_effective_stop_price": float(protection.confirmed_stop_price or 0.0),
                        "exchange_confirmed_stop_price": float(protection.confirmed_stop_price or 0.0),
                        "exchange_protection_status": (
                            "STOP_CONFIRMED" if protection.protected else str(
                                protection.reason or protection.protection_state or ""
                            )
                        ),
                        "exchange_protection_algo_ids": [
                            value for value in [protection.sl_algo_id, *list(protection.tp_algo_ids or [])]
                            if value
                        ],
                        "exchange_tp_algo_id": str((protection.tp_algo_ids or [""])[0] or ""),
                        "tp_owner": str(request.metadata.get("tp_owner", "") or ""),
                        "tp_mode_at_entry": str(request.metadata.get("tp_mode_at_entry", "") or ""),
                        "local_tp1_enabled": bool(request.metadata.get("local_tp1_enabled", False)),
                        "local_tp2_enabled": bool(request.metadata.get("local_tp2_enabled", False)),
                        "tp_owner_timestamp": str(request.metadata.get("tp_owner_timestamp", request.metadata.get("ownership_timestamp", "")) or ""),
                        "partials_enabled_at_entry": bool(request.metadata.get("partials_enabled_at_entry", False)),
                    },
                )
                if not updated:
                    row = self._mark_recovery_pending(
                        request, state="PARTIALLY_FILLED",
                        reason="recovered_filled_registry_activation_pending",
                        filled_contracts=fill_qty,
                        average_fill_price=avg_px,
                        exchange_order_id=order_id,
                        protection=protection.__dict__,
                        registry_activation_error="durable_recovered_registry_protection_lineage_not_confirmed",
                        recovery_evidence={"order": order, "fills": matching_fills,
                                           "positions": live_positions,
                                           "protection": protection.__dict__},
                    )
                    return self._result_from_row(row, "recovered_filled_registry_activation_pending", ok=False)
            partial_remainder = cancellation_remainder_outstanding and fill_qty < requested_qty
            if partial_remainder and order_state in {"live", "effective", "partially_filled"}:
                row = self._mark_recovery_pending(
                    request, state="CANCEL_PENDING",
                    reason="partial_remainder_cancel_still_outstanding",
                    filled_contracts=fill_qty, average_fill_price=avg_px,
                    exchange_order_id=order_id, protection=protection.__dict__,
                    recovery_evidence={"order": order, "fills": matching_fills, "positions": live_positions},
                )
                return self._result_from_row(row, "partial_remainder_cancel_still_outstanding", ok=False)
            if authoritative_partial_cancel:
                final = await self._finalize_terminal(
                    request, state="CANCELLED", terminal_class="cancelled",
                    reason="partial_execution_remainder_cancelled", exchange_contacted=True,
                    fields={**partial_cancel_facts, "protection": protection.__dict__},
                )
                return self._result_from_row(final, "partial_execution_remainder_cancelled", ok=False)
            final = await self._finalize_terminal(
                request, state="FILLED", terminal_class="filled", reason="recovered_exchange_fill",
                exchange_contacted=True,
                fields={"filled_contracts": fill_qty, "average_fill_price": avg_px,
                        "exchange_order_id": order_id, "protection": protection.__dict__},
            )
            return self._result_from_row(final, "recovered_exchange_fill", ok=True)

        if order_state in {"live", "effective", "partially_filled"}:
            if not self._ensure_recovered_open_risk_slot(request.intent_id):
                row = self._mark_recovery_pending(
                    request, state="ACKNOWLEDGED", reason="recovered_acknowledged_risk_slot_open_pending",
                    exchange_order_id=order_id, exchange_contacted=True,
                    recovery_status=order,
                    recovery_evidence={"order": order, "fills": [], "positions": []},
                )
                return self._result_from_row(row, "recovered_acknowledged_risk_slot_open_pending", ok=False)
            row = self._mark_recovery_pending(
                request, state="ACKNOWLEDGED", reason="recovered_acknowledged_unfilled",
                exchange_order_id=order_id, exchange_contacted=True,
                recovery_status=order,
                recovery_evidence={"order": order, "fills": [], "positions": []},
            )
            return self._result_from_row(row, "recovered_existing_order", ok=False)

        if order_state in {"canceled", "cancelled", "rejected", "expired", "mmp_canceled"}:
            mapped = "CANCELLED" if order_state in {"canceled", "cancelled", "expired", "mmp_canceled"} else "REJECTED"
            if mapped == "CANCELLED" and fill_evidence != FILL_KNOWN_ZERO:
                row = self._mark_recovery_pending(
                    request, state="CANCEL_PENDING",
                    reason="recovered_terminal_cancel_cumulative_evidence_unknown",
                    exchange_order_id=order_id, recovery_status=order,
                    cumulative_fill_evidence=fill_evidence,
                )
                return self._result_from_row(row, "recovered_terminal_cancel_cumulative_evidence_unknown", ok=False)
            if mapped == "CANCELLED" and not self._prepare_zero_fill_risk_slot(request.intent_id):
                row = self._mark_recovery_pending(
                    request, state="CANCEL_PENDING",
                    reason="recovered_authoritative_cancel_risk_slot_close_persistence_pending",
                    exchange_order_id=order_id, recovery_status=order,
                )
                return self._result_from_row(row, "recovered_authoritative_cancel_risk_slot_close_persistence_pending", ok=False)
            row = await self._finalize_terminal(
                request, state=mapped,
                terminal_class="cancelled" if mapped == "CANCELLED" else "exchange_rejection",
                reason="recovered_cancelled" if mapped == "CANCELLED" else "recovered_exchange_rejection",
                exchange_contacted=True,
                fields={"exchange_order_id": order_id, "recovery_status": order,
                        "cumulative_fill_evidence": fill_evidence,
                        "recovery_evidence": {"order": order, "fills": [], "positions": []}},
            )
            if mapped == "CANCELLED":
                self._reconcile_authoritative_zero_fill_risk_slot(request.intent_id)
            return self._result_from_row(row, "recovered_terminal_nonfill", ok=False)

        # Empty bounded readback is not evidence that a POST did not mutate
        # the exchange.  Keep the durable intent, reservation, risk slot, and
        # retry ownership intact; recovery is read-only and never reposts.
        row = self._mark_recovery_pending(
            request,
            state="SUBMIT_UNCERTAIN",
            reason="uncertain_submission_no_authoritative_evidence",
            exchange_order_id=order_id,
            cumulative_fill_evidence=cumulative_evidence,
            recovery_evidence={
                "order_detail": order_detail_evidence,
                "order": {},
                "fills": [],
                "positions": [],
                "history_rows": history_rows,
                "recovery_action": "retain_exchange_owned_uncertain_submission",
            },
        )
        return self._result_from_row(row, "uncertain_submission_no_authoritative_evidence", ok=False)

    async def _recover_manual_close(self, existing: dict) -> ExecutionResult:
        """Reconcile an uncertain operator close without constructing a POST."""
        intent_id = str(existing.get("intent_id", "") or "")
        coin = str(existing.get("coin", "") or "").upper()
        request = self._request_from_row(existing)
        truth = await self._operator_position_truth(coin)
        expected_lifecycle = str(existing.get("position_lifecycle_id", "") or "")
        if truth.get("ok") and float(truth.get("contracts", 0.0) or 0.0) == 0.0:
            if not self._reconcile_closed_position_risk_slot(coin):
                row = self._mark_recovery_pending(
                    request, state="SUBMIT_UNCERTAIN",
                    reason="manual_close_position_risk_slot_close_pending",
                    recovery_status=truth, recovery_only=True,
                )
                return self._result_from_row(row, "manual_close_position_risk_slot_close_pending", ok=False)
            row = await self._finalize_terminal(
                request, state="FILLED", terminal_class="filled",
                reason="manual_close_recovered_exchange_zero", exchange_contacted=True,
                fields={"exchange_closed": True, "recovery_status": truth, "recovery_only": True},
            )
            return self._result_from_row(row, "manual_close_recovered_exchange_zero", ok=True)
        if truth.get("ok") and truth.get("position") is not None:
            current_lifecycle = str(
                self._operator_lifecycle_context(coin, truth.get("position")).get("current_id", "")
                or ""
            )
            if expected_lifecycle and current_lifecycle and current_lifecycle != expected_lifecycle:
                row = await self._finalize_terminal(
                    request, state="FAILED_FINAL", terminal_class="reconciliation_terminal",
                    reason="manual_close_belongs_to_previous_position_lifecycle", exchange_contacted=True,
                    fields={"recovery_status": truth, "recovery_only": True,
                            "mutation_outcome": "LIFECYCLE_MISMATCH",
                            "current_position_lifecycle_id": current_lifecycle},
                )
                return self._result_from_row(row, "manual_close_lifecycle_mismatch", ok=False)
        row = self.orders.transition(
            intent_id, "SUBMIT_UNCERTAIN",
            recovery_status=truth, recovery_only=True,
        )
        return self._result_from_row(row, "manual_close_recovery_pending", ok=False)

    async def recover_unresolved(self) -> list[ExecutionResult]:
        """Reconcile unresolved mutation intents without ever reposting them."""
        results: list[ExecutionResult] = []
        # Rehydrate ledger capacity from durable position lineage before any
        # recovered intent can proceed.  This is local accounting only: it
        # does not guess a closed position and never posts an entry.
        try:
            from position_registry import ensure_open_risk_slot, get_active_details
            for coin in get_active_details():
                ensure_open_risk_slot(coin)
        except Exception:
            logger.exception("Startup position risk-slot reconciliation pending")
        # Terminal accounting side effects have their own durable recovery
        # owner.  This local-only pass neither queries nor mutates OKX.
        for terminal in self.orders.terminal_cleanup_pending():
            await self._retry_terminal_cleanup(terminal)
        for terminal in self.orders.terminal_telemetry_pending():
            await self._flush_terminal_telemetry(str(terminal.get("intent_id", "") or ""))
        for row in self.orders.risk_slot_pending():
            intent_id = str(row.get("intent_id", "") or "")
            for action, operation in dict(row.get("risk_slot_operations", {}) or {}).items():
                if str(dict(operation or {}).get("status", "")) in {"pending", "failed"}:
                    self._apply_risk_slot_operation(intent_id, str(action))
            self._reconcile_authoritative_zero_fill_risk_slot(intent_id)
        for existing in self.orders.unresolved():
            if str(existing.get("order_type", "") or "") == "staged":
                request = self._request_from_row(existing)
                async with self._lock(request.intent_id):
                    results.append(await self._resume_staged_parent(request, existing))
                continue
            fallback_plan = dict(existing.get("fallback_plan", {}) or {})
            fallback_child_id = str(existing.get("fallback_child_intent_id", "") or "")
            if fallback_plan:
                parent_request = self._request_from_row(existing)
                # A plan is durable before original slot closure.  On restart
                # finish that close before creating/linking/submitting a child.
                confirmation = dict(existing.get("cancel_confirmation", {}) or {})
                confirmation_state = str(confirmation.get("state", "") or "").lower()
                fill_evidence, _confirmation_fill = _authoritative_cumulative_fill(confirmation)
                if (
                    confirmation_state not in {"canceled", "cancelled", "expired", "mmp_canceled"}
                    or fill_evidence != FILL_KNOWN_ZERO
                ):
                    continue
                if not self._reconcile_authoritative_zero_fill_risk_slot(parent_request.intent_id, authoritative=True):
                    continue
                planned_child_id = str(fallback_plan.get("child_intent_id", "") or "")
                child = self.orders.get(planned_child_id) if planned_child_id else None
                if not child:
                    fallback, status = self._establish_fallback_child(parent_request, existing)
                    if fallback is not None:
                        results.append(await self.submit(fallback))
                    # A persistence/link failure remains local and recoverable;
                    # no child may have crossed a physical mutation boundary.
                    continue
                if not fallback_child_id:
                    try:
                        self.orders.transition(
                            parent_request.intent_id, "CANCEL_PENDING",
                            fallback_child_intent_id=planned_child_id,
                            fallback_establishment_status="established",
                            fallback_establishment_error="",
                        )
                    except Exception:
                        continue
                if not child.get("terminal_finalized_at"):
                    child_state = str(child.get("state", "") or "")
                    if child_state in {"INTENT_CREATED", "CAPITAL_RESERVED", "ROUTER_VALIDATING", "PAYLOAD_READY"}:
                        results.append(await self.submit(self._fallback_request(parent_request, planned_child_id)))
                    continue
                await self._finalize_terminal(
                    parent_request, state="CANCELLED", terminal_class="cancelled",
                    reason="authoritative_cancel_before_fallback_recovered", exchange_contacted=True,
                    fields={"fallback_started": True, "fallback_child_intent_id": planned_child_id,
                            "fallback_terminal_state": child.get("state")},
                )
                continue
            if fallback_child_id:
                child = self.orders.get(fallback_child_id) or {}
                if not child:
                    # A historical link to an absent child is provably before
                    # that child's boundary.  Preserve original cancel truth
                    # and free the parent rather than leaking its reservation.
                    await self._finalize_terminal(
                        self._request_from_row(existing), state="CANCELLED", terminal_class="cancelled",
                        reason="fallback_child_missing_pre_submission", exchange_contacted=True,
                        fields={"fallback_child_intent_id": fallback_child_id,
                                "fallback_establishment_status": "abandoned_missing_child"},
                    )
                    continue
                if not child.get("terminal_finalized_at"):
                    # The canceled parent is only a lifecycle container while
                    # the linked fallback is exchange-owned recovery work.
                    continue
            if str(existing.get("kind", "") or "") == "manual_close":
                results.append(await self._recover_manual_close(existing))
                continue
            state = str(existing.get("state", "") or "")
            if state not in {"PAYLOAD_READY", "SUBMISSION_ATTEMPTED", "SUBMITTED", "ACKNOWLEDGED", "SUBMIT_UNCERTAIN", "RECOVERY_QUERY", "CANCEL_PENDING", "PARTIALLY_FILLED"}:
                continue
            request = ExecutionRequest(
                intent_id=str(existing.get("intent_id", "")),
                candidate_id=str(existing.get("candidate_id", "")),
                reservation_id=str(existing.get("reservation_id", "")),
                mode=str(existing.get("mode", "Guarded")),
                engine=str(existing.get("engine", "RECOVERED")),
                coin=str(existing.get("coin", "")),
                direction=str(existing.get("direction", "long")),
                requested_contracts=float(existing.get("requested_contracts", 0.0) or 0.0),
                approved_contracts=float(existing.get("approved_contracts", 0.0) or 0.0),
                approved_notional_usdt=float(existing.get("approved_notional_usdt", 0.0) or 0.0),
                order_type=str(existing.get("order_type", "market")),
                price=float(existing.get("price", 0.0) or 0.0),
                stop_price=float(existing.get("stop_price", 0.0) or 0.0),
                take_profit=float(existing.get("take_profit", 0.0) or 0.0),
                reduce_only=bool(existing.get("reduce_only", False)),
                margin_mode=str(existing.get("margin_mode", "isolated")),
                leverage=int(existing.get("leverage", 1) or 1),
                requested_leverage=(int(existing.get("requested_leverage")) if existing.get("requested_leverage") not in (None, "") else None),
                exchange_configured_leverage=(int(existing.get("exchange_configured_leverage")) if existing.get("exchange_configured_leverage") not in (None, "") else None),
                position_effective_leverage=(int(existing.get("position_effective_leverage")) if existing.get("position_effective_leverage") not in (None, "") else None),
                leverage_truth_status=str(existing.get("leverage_truth_status", "") or ""),
                leverage_truth_timestamp=str(existing.get("leverage_truth_timestamp", "") or ""),
                leverage_truth_source=str(existing.get("leverage_truth_source", "") or ""),
                leverage_truth_mismatch=bool(existing.get("leverage_truth_mismatch", False)),
                requested_vs_configured_mismatch=bool(existing.get("requested_vs_configured_mismatch", False)),
                configured_vs_effective_mismatch=bool(existing.get("configured_vs_effective_mismatch", False)),
                timeout_secs=float(existing.get("timeout_secs", 15.0) or 15.0),
                retry_count=int(existing.get("retry_count", 1) or 1),
                fallback_mode=str(existing.get("fallback_mode", "market")),
                metadata=dict(existing.get("metadata", {}) or {}),
            )
            async with self._lock(request.intent_id):
                if state == "PAYLOAD_READY":
                    row = await self._finalize_terminal(
                        request, state="FAILED_FINAL", terminal_class="local_rejection",
                        reason="recovery_payload_ready_never_submitted",
                        exchange_contacted=False,
                        fields={"recovery_only": True, "mutation_attempted": False,
                                "recovery_evidence": {"restart_state": state}},
                    )
                    self._release_pre_submit_registry(request.coin)
                    results.append(self._result_from_row(row, "recovery_payload_ready_never_submitted", ok=False))
                else:
                    recovered = await self._recover_uncertain(existing, request)
                    results.append(recovered)
                    child_row = self.orders.get(request.intent_id) or {}
                    parent_id = str(
                        dict(child_row.get("metadata", {}) or {}).get("fallback_parent_intent_id", "")
                        or ""
                    )
                    if parent_id and child_row.get("terminal_finalized_at"):
                        parent = self.orders.get(parent_id) or {}
                        if parent and not parent.get("terminal_finalized_at"):
                            await self._finalize_terminal(
                                self._request_from_row(parent), state="CANCELLED",
                                terminal_class="cancelled",
                                reason="authoritative_cancel_before_fallback_recovered",
                                exchange_contacted=True,
                                fields={"fallback_started": True,
                                        "fallback_child_intent_id": request.intent_id,
                                        "fallback_terminal_state": child_row.get("state")},
                            )
        return results

    def _apply_risk_slot_operation(self, intent_id: str, action: str) -> bool:
        """Apply one durable risk operation through the idempotent sink.

        The operation is persisted *before* this call.  If its completion
        marker cannot be saved, the sink remembers the deterministic ID and a
        retry is a no-op rather than a second increment/decrement.
        """
        row = self.orders.get(intent_id) or {}
        operation = dict(dict(row.get("risk_slot_operations", {}) or {}).get(action, {}) or {})
        operation_id = str(operation.get("operation_id", "") or "")
        if not operation_id:
            return False
        if operation.get("status") == "complete":
            return True
        try:
            from risk_manager import risk
            risk.apply_slot_operation(operation_id, action)
        except Exception as exc:
            try:
                self.orders.mark_risk_slot_operation(
                    intent_id, operation_id=operation_id, action=action,
                    status="pending", error=f"{type(exc).__name__}:{exc}",
                )
            except Exception:
                logger.exception("Risk slot %s failure could not be persisted for %s", action, intent_id)
            return False
        try:
            self.orders.mark_risk_slot_operation(
                intent_id, operation_id=operation_id, action=action, status="complete",
            )
        except Exception:
            # Keep the durable event pending.  The sink's operation identity
            # makes recovery/restart safe even though this acknowledgement did
            # not persist.
            logger.exception("Risk slot %s acknowledgement pending for %s", action, intent_id)
            return False
        return True

    def _risk_slot_owner_intent(self, intent_id: str, row: dict | None = None) -> str:
        """Resolve the immutable position identity for a slot operation.

        Staged children may build one exchange position together.  Their
        capacity must therefore belong to the parent manifest's resulting
        position identity, not to whichever child last overwrote the local
        registry row.  A child-provided parent name is insufficient: this
        accepts the shared identity only when the durable parent manifest
        contains the exact child and reservation relationship.
        """
        intent_id = str(intent_id or "")
        row = dict(row or self.orders.get(intent_id) or {})
        metadata = dict(row.get("metadata", {}) or {})
        parent_id = str(metadata.get("staged_parent_intent_id", "") or "")
        if not parent_id or parent_id == intent_id:
            return intent_id
        parent = self.orders.get(parent_id) or {}
        manifest = dict(parent.get("staged_manifest", {}) or {})
        children = list(manifest.get("children", []) or [])
        child_exists = any(str(entry.get("intent_id", "") or "") == intent_id for entry in children)
        if (
            child_exists
            and str(manifest.get("staged_parent_intent_id", "") or "") == parent_id
            and str(manifest.get("reservation_owner_intent_id", "") or "") == parent_id
            and str(parent.get("reservation_id", "") or "") == str(row.get("reservation_id", "") or "")
            and str(metadata.get("reservation_owner_intent_id", "") or "") == parent_id
        ):
            return parent_id
        return intent_id

    @staticmethod
    def _risk_slot_operation(operation_owner: str, action: str) -> dict:
        return {
            "operation_id": f"risk-{action}:{operation_owner}",
            "action": action,
            "status": "pending",
            "error": "",
            "created_at": time.time(),
        }

    def _prepare_open_risk_slot(self, intent_id: str) -> bool:
        """Persist a restart-safe shared open operation before applying it."""
        row = self.orders.get(intent_id) or {}
        if not row or row.get("terminal_finalized_at"):
            return False
        owner = self._risk_slot_owner_intent(intent_id, row)
        targets = [intent_id] if owner == intent_id else [owner, intent_id]
        operation_id = f"risk-open:{owner}"
        for target in targets:
            target_row = self.orders.get(target) or {}
            if not target_row or target_row.get("terminal_finalized_at"):
                return False
            operations = dict(target_row.get("risk_slot_operations", {}) or {})
            operation = dict(operations.get("open", {}) or {})
            if str(operation.get("operation_id", "") or "") != operation_id:
                operations["open"] = self._risk_slot_operation(owner, "open")
            try:
                self.orders.transition(
                    target, str(target_row.get("state", "ACKNOWLEDGED") or "ACKNOWLEDGED"),
                    risk_slot_acquired=True,
                    risk_slot_operations=operations,
                    risk_slot_status=str(operations["open"].get("status", "pending") or "pending"),
                    risk_slot_error=str(operations["open"].get("error", "") or ""),
                    risk_slot_owner_intent_id=owner,
                )
            except Exception:
                logger.exception("Risk-slot open persistence pending for %s", target)
                return False
        return True

    def _ensure_recovered_open_risk_slot(self, intent_id: str) -> bool:
        """Restore local capacity for an exchange-proven surviving order.

        This has no exchange side effect.  It closes the POST-to-local-marker
        crash window by durably creating the deterministic open operation
        before applying it to the restart-safe risk ledger.
        """
        row = self.orders.get(intent_id) or {}
        if not row or row.get("terminal_finalized_at"):
            return False
        owner = self._risk_slot_owner_intent(intent_id, row)
        operation = dict(dict(row.get("risk_slot_operations", {}) or {}).get("open", {}) or {})
        if (
            not bool(row.get("risk_slot_acquired"))
            or str(operation.get("operation_id", "") or "") != f"risk-open:{owner}"
        ) and not self._prepare_open_risk_slot(intent_id):
            return False
        return self._apply_risk_slot_operation(intent_id, "open")

    def _prepare_zero_fill_risk_slot(self, intent_id: str) -> bool:
        """Persist the deterministic close operation before its counter sink."""
        row = self.orders.get(intent_id) or {}
        if not bool(row.get("risk_slot_acquired")):
            return True
        owner = self._risk_slot_owner_intent(intent_id, row)
        # A staged child is not a position lifecycle on its own.  Its zero-fill
        # cancellation must never close the shared parent slot while a later
        # child can still produce the resulting position.
        if owner != intent_id:
            return True
        if dict(row.get("risk_slot_operations", {}) or {}).get("close"):
            return True
        try:
            self.orders.prepare_risk_slot_operation(
                intent_id, operation_id=f"risk-close:{owner}", action="close",
            )
        except Exception:
            logger.exception("Risk-slot close intent persistence pending for %s", intent_id)
            return False
        return True

    def _reconcile_authoritative_zero_fill_risk_slot(self, intent_id: str, *, authoritative: bool = False) -> bool:
        """Release only an authoritatively cancelled, actually zero-fill slot."""
        row = self.orders.get(intent_id) or {}
        if str(row.get("state", "") or "") != "CANCELLED" and not (
            authoritative and bool(row.get("authoritative_cancellation"))
        ):
            return False
        if str(row.get("cumulative_fill_evidence", "") or "") != FILL_KNOWN_ZERO:
            return False
        if not bool(row.get("risk_slot_acquired")):
            return True
        if self._risk_slot_owner_intent(intent_id, row) != intent_id:
            return True
        if not self._prepare_zero_fill_risk_slot(intent_id):
            return False
        return self._apply_risk_slot_operation(intent_id, "close")

    @staticmethod
    def _reconcile_closed_position_risk_slot(coin: str) -> bool:
        """Close the entry slot only after a caller has proven position zero."""
        try:
            from position_registry import close_authoritative_risk_slot
            return bool(close_authoritative_risk_slot(coin))
        except Exception:
            logger.exception("Position risk-slot reconciliation pending for %s", coin)
            return False

    def _upsert_surviving_partial_cancel_position(
        self,
        request: ExecutionRequest,
        *,
        filled_contracts: float,
        average_fill_price: float,
        order_id: str,
        leverage: int,
        leverage_metadata: dict | None = None,
        protection=None,
        protection_status: str = "",
        recovered: bool = False,
    ) -> None:
        """Use the recovered-position registry semantics for every partial cancel.

        The authoritative upsert creates a missing row or replaces exact
        exposure; repeated recovery never aggregates the quantity.
        """
        from position_registry import upsert_authoritative_position

        try:
            lifecycle_leverage = int(leverage)
        except (TypeError, ValueError):
            raise RuntimeError("partial_cancel_effective_leverage_unavailable")
        if lifecycle_leverage <= 0:
            raise RuntimeError("partial_cancel_effective_leverage_unavailable")
        durable = self.orders.get(request.intent_id) or {}
        metadata = {
            "requested_leverage": request.requested_leverage,
            "exchange_configured_leverage": request.exchange_configured_leverage,
            "position_effective_leverage": lifecycle_leverage,
            "leverage_truth_status": request.leverage_truth_status or durable.get("leverage_truth_status", ""),
            "leverage_truth_timestamp": request.leverage_truth_timestamp or durable.get("leverage_truth_timestamp", ""),
            "leverage_truth_source": request.leverage_truth_source or durable.get("leverage_truth_source", ""),
            "leverage_truth_mismatch": bool(request.leverage_truth_mismatch),
            "requested_vs_configured_mismatch": bool(request.requested_vs_configured_mismatch),
            "configured_vs_effective_mismatch": bool(request.configured_vs_effective_mismatch),
            **dict(leverage_metadata or {}),
            "pending_reservation": False,
            "entry_lifecycle": "FILLED",
            "mutation_attempted": True,
            "capital_reservation_id": request.reservation_id,
            "execution_intent_id": request.intent_id,
            "risk_slot_intent_id": self._risk_slot_owner_intent(request.intent_id),
            "candidate_id": request.candidate_id,
            "origin_entry_order_id": order_id,
            "origin_entry_client_id": str(durable.get("client_order_id", "") or deterministic_client_order_id(request.intent_id)),
            "exchange_confirmed": True,
            "partial_execution": True,
            "unfilled_remainder_cancelled": True,
            "actual_filled_contracts": float(filled_contracts),
            "requested_contracts": float(request.requested_contracts or request.approved_contracts or 0.0),
            "pos_side": "long" if request.direction.lower() in {"long", "buy"} else "short",
            "recovered_from_uncertain": bool(recovered),
            "desired_stop_price": float(request.stop_price or 0.0),
            "exchange_protection_status": "PROTECTION_INTENT_PENDING",
            "tp_owner": str(request.metadata.get("tp_owner", "") or ""),
            "tp_mode_at_entry": str(request.metadata.get("tp_mode_at_entry", "") or ""),
            "local_tp1_enabled": bool(request.metadata.get("local_tp1_enabled", False)),
            "local_tp2_enabled": bool(request.metadata.get("local_tp2_enabled", False)),
            "tp_owner_timestamp": str(request.metadata.get("tp_owner_timestamp", request.metadata.get("ownership_timestamp", "")) or ""),
            "partials_enabled_at_entry": bool(request.metadata.get("partials_enabled_at_entry", False)),
        }
        if protection_status:
            metadata["exchange_protection_status"] = str(protection_status)
        if protection is not None:
            metadata.update({
                "local_effective_stop_price": float(getattr(protection, "confirmed_stop_price", 0.0) or 0.0),
                "exchange_confirmed_stop_price": float(getattr(protection, "confirmed_stop_price", 0.0) or 0.0),
                "exchange_protection_status": (
                    "STOP_CONFIRMED" if bool(getattr(protection, "protected", False)) else str(
                        getattr(protection, "reason", "") or getattr(protection, "protection_state", "") or "PROTECTION_FAILED"
                    )
                ),
                "exchange_protection_algo_ids": [
                    value for value in [
                        getattr(protection, "sl_algo_id", ""),
                        *list(getattr(protection, "tp_algo_ids", []) or []),
                    ] if value
                ],
                "exchange_tp_algo_id": str((getattr(protection, "tp_algo_ids", []) or [""])[0] or ""),
            })
        instrument = self.instruments.require(request.coin)
        if average_fill_price <= 0:
            raise RuntimeError("partial_cancel_authoritative_fill_price_unavailable")
        filled_notional = instrument.contracts_to_notional(
            filled_contracts, average_fill_price,
        )
        updated = upsert_authoritative_position(
            request.coin,
            direction="long" if request.direction.lower() in {"long", "buy"} else "short",
            engine=str(request.engine or "RECOVERED"), size_usdt=filled_notional,
            entry=average_fill_price, leverage=lifecycle_leverage,
            stop_loss=float(request.stop_price), take_profit=float(request.take_profit),
            order_id=order_id, metadata=metadata,
        )
        if not updated:
            raise RuntimeError("position_registry_missing_for_partial_cancel")

    async def _converge_authoritative_partial_cancel(
        self, request: ExecutionRequest, status: dict, cancel_response: dict, order_id: str,
    ) -> dict:
        """Preserve cancel truth while protecting the already-filled position."""
        durable = self.orders.get(request.intent_id) or {}
        fill_evidence, cumulative_fill = _authoritative_cumulative_fill(status)
        if fill_evidence == FILL_UNKNOWN:
            return self.orders.transition(
                request.intent_id, "CANCEL_PENDING", cancel_response=cancel_response,
                cancel_confirmation=status, recovery_required=True,
                recovery_reason="authoritative_cancel_cumulative_evidence_unknown",
                cumulative_fill_evidence=FILL_UNKNOWN,
            )
        filled = max(float(cumulative_fill or 0.0), float(durable.get("filled_contracts", 0.0) or 0.0))
        average_price = float(status.get("avgPx", status.get("fillPx", 0.0)) or 0.0)
        if average_price <= 0:
            average_price = float(durable.get("average_fill_price", 0.0) or 0.0)
        if (
            durable.get("terminal_finalized_at")
            and str(durable.get("state", "") or "") == "CANCELLED"
            and bool(durable.get("partial_execution"))
        ):
            # Direct duplicate cancellation/restart observation is already
            # converged.  Do not attach protection or mutate accounting twice.
            return durable
        requested = float(request.requested_contracts or request.approved_contracts or 0.0)
        if filled >= requested > 0:
            # A cancel acknowledgement can race an order that fully filled.
            # It is not a zero-fill cancellation: preserve ownership until
            # canonical full-fill registry/protection convergence completes.
            return self.orders.transition(
                request.intent_id, "PARTIALLY_FILLED",
                cancel_response=cancel_response, cancel_confirmation=status,
                exchange_order_id=order_id, filled_contracts=filled,
                average_fill_price=average_price,
                cumulative_fill_evidence=fill_evidence,
                recovery_required=True,
                recovery_reason="authoritative_cancel_full_fill_requires_filled_convergence",
            )
        if filled <= 0:
            if fill_evidence == FILL_KNOWN_ZERO and not self._prepare_zero_fill_risk_slot(request.intent_id):
                return self.orders.transition(
                    request.intent_id, "CANCEL_PENDING", cancel_response=cancel_response,
                    cancel_confirmation=status, recovery_required=True,
                    recovery_reason="authoritative_cancel_risk_slot_close_persistence_pending",
                )
            return await self._finalize_terminal(
                request, state="CANCELLED", terminal_class="cancelled",
                reason="authoritative_cancel_confirmation", exchange_contacted=True,
                fields={"cancel_response": cancel_response, "cancel_confirmation": status,
                        "exchange_order_id": order_id, "filled_contracts": filled,
                        "average_fill_price": average_price, "requested_contracts": request.requested_contracts,
                        "cumulative_fill_evidence": fill_evidence},
            )
        leverage_truth = {}
        effective_leverage = None
        try:
            validated_request_leverage = int(request.position_effective_leverage or 0)
        except (TypeError, ValueError):
            validated_request_leverage = 0
        if (
            validated_request_leverage > 0
            and str(request.leverage_truth_status or "").upper() == "POSITION_EFFECTIVE"
        ):
            effective_leverage = validated_request_leverage
            leverage_truth = {
                "position_effective_leverage": effective_leverage,
                "leverage_truth_status": request.leverage_truth_status,
                "leverage_truth_timestamp": request.leverage_truth_timestamp,
                "leverage_truth_source": request.leverage_truth_source,
                "leverage_truth_mismatch": request.leverage_truth_mismatch,
                "requested_vs_configured_mismatch": request.requested_vs_configured_mismatch,
                "configured_vs_effective_mismatch": request.configured_vs_effective_mismatch,
            }
        elif not self.paper_mode():
            try:
                leverage_truth = await self._position_effective_leverage(request)
            except Exception as exc:
                leverage_truth = {
                    "position_effective_leverage": None,
                    "leverage_truth_status": "LEVERAGE_UNKNOWN",
                    "leverage_truth_source": f"partial_cancel_position_truth_error:{type(exc).__name__}",
                }
        if effective_leverage is None:
            effective_leverage = leverage_truth.get("position_effective_leverage")
        if not effective_leverage or int(effective_leverage) <= 0:
            return self.orders.transition(
                request.intent_id, "PARTIALLY_FILLED",
                cancel_response=cancel_response, cancel_confirmation=status,
                exchange_order_id=order_id, filled_contracts=filled,
                average_fill_price=average_price, partial_execution=True,
                unfilled_remainder_cancelled=True, authoritative_cancellation=True,
                recovery_required=True,
                recovery_reason="partial_cancel_effective_leverage_unknown",
                cumulative_fill_evidence=fill_evidence,
                recovery_evidence={"leverage_truth": leverage_truth},
            )
        leverage_metadata = {
            "requested_leverage": request.requested_leverage,
            "exchange_configured_leverage": request.exchange_configured_leverage,
            "position_effective_leverage": int(effective_leverage),
            "leverage_truth_status": str(leverage_truth.get("leverage_truth_status", "POSITION_EFFECTIVE") or "POSITION_EFFECTIVE"),
            "leverage_truth_timestamp": str(leverage_truth.get("leverage_truth_timestamp", "") or ""),
            "leverage_truth_source": str(leverage_truth.get("leverage_truth_source", "") or ""),
            "leverage_truth_mismatch": bool(leverage_truth.get("leverage_truth_mismatch", False)),
            "requested_vs_configured_mismatch": bool(leverage_truth.get("requested_vs_configured_mismatch", False)),
            "configured_vs_effective_mismatch": bool(leverage_truth.get("configured_vs_effective_mismatch", False)),
        }
        request = replace(
            request,
            leverage=int(effective_leverage),
            position_effective_leverage=int(effective_leverage),
            leverage_truth_status=leverage_metadata["leverage_truth_status"],
            leverage_truth_timestamp=leverage_metadata["leverage_truth_timestamp"],
            leverage_truth_source=leverage_metadata["leverage_truth_source"],
            metadata={**dict(request.metadata or {}), **leverage_metadata},
        )
        prior_cumulative = float(
            durable.get("authoritative_cumulative_fill", durable.get("filled_contracts", 0.0)) or 0.0
        )
        fill_delta = max(filled - prior_cumulative, 0.0)
        if fill_delta > 0:
            self.orders.add_fill(request.intent_id, {
                "fillId": status.get("tradeId", f"ORD-{order_id}-CUM-{filled}"), "fillSz": fill_delta,
                "fillPx": average_price, "fee": status.get("fee", 0),
            })
        self.orders.transition(
            request.intent_id, "PARTIALLY_FILLED", filled_contracts=filled,
            average_fill_price=average_price, exchange_order_id=order_id,
            cancel_confirmation=status, authoritative_cumulative_fill=filled,
            cumulative_fill_evidence=fill_evidence,
        )
        try:
            self._upsert_surviving_partial_cancel_position(
                request, filled_contracts=filled, average_fill_price=average_price,
                order_id=order_id, leverage=int(effective_leverage),
                leverage_metadata=leverage_metadata,
            )
        except Exception as exc:
            return self.orders.transition(
                request.intent_id, "PARTIALLY_FILLED", cancel_response=cancel_response,
                cancel_confirmation=status, exchange_order_id=order_id,
                requested_contracts=request.requested_contracts, filled_contracts=filled,
                average_fill_price=average_price, partial_execution=True,
                unfilled_remainder_cancelled=True, authoritative_cancellation=True,
                recovery_required=True, recovery_reason="partial_cancel_registry_initial_write_pending",
                position_registry_error=f"{type(exc).__name__}:{exc}",
            )
        try:
            protection = await self.protection.ensure(
                coin=request.coin, direction=request.direction, contracts=filled,
                stop_price=request.stop_price, take_profit=request.take_profit,
                client_order_id=str(status.get("clOrdId", "") or deterministic_client_order_id(request.intent_id)),
                paper=False, tp_owner=str(request.metadata.get("tp_owner", "") or ""),
            )
        except Exception as exc:
            protection = None
            protection_error = f"{type(exc).__name__}:{exc}"
        else:
            protection_error = ""
        evidence = {
            "cancel_response": cancel_response, "cancel_confirmation": status,
            "exchange_order_id": order_id, "requested_contracts": request.requested_contracts,
            "filled_contracts": filled, "average_fill_price": average_price,
            "partial_execution": True, "unfilled_remainder_cancelled": True,
        }
        if protection is None or not protection.protected:
            evidence["protection"] = getattr(protection, "__dict__", {})
            if protection_error:
                evidence["protection_error"] = protection_error
            protection_status = str(
                protection_error or getattr(protection, "reason", "")
                or getattr(protection, "protection_state", "") or "PROTECTION_FAILED"
            )
            try:
                self._upsert_surviving_partial_cancel_position(
                    request, filled_contracts=filled, average_fill_price=average_price,
                    order_id=order_id, leverage=int(effective_leverage),
                    leverage_metadata=leverage_metadata, protection=protection,
                    protection_status=protection_status,
                )
            except Exception as exc:
                return self.orders.transition(
                    request.intent_id, "PARTIALLY_FILLED", cancel_response=cancel_response,
                    cancel_confirmation=status, exchange_order_id=order_id,
                    requested_contracts=request.requested_contracts, filled_contracts=filled,
                    average_fill_price=average_price, partial_execution=True,
                    unfilled_remainder_cancelled=True, authoritative_cancellation=True,
                    protection=getattr(protection, "__dict__", {}), recovery_required=True,
                    recovery_reason="partial_cancel_registry_protection_status_write_pending",
                    position_registry_error=f"{type(exc).__name__}:{exc}",
                )
            return await self._finalize_terminal(
                request, state="CANCELLED", terminal_class="protection_failure",
                reason="partial_cancel_protection_failure", exchange_contacted=True, fields=evidence,
            )
        try:
            self._upsert_surviving_partial_cancel_position(
                request, filled_contracts=filled, average_fill_price=average_price,
                order_id=order_id, leverage=int(effective_leverage),
                leverage_metadata=leverage_metadata, protection=protection,
            )
        except Exception as exc:
            return self.orders.transition(
                request.intent_id, "PARTIALLY_FILLED", cancel_response=cancel_response,
                cancel_confirmation=status, exchange_order_id=order_id,
                requested_contracts=request.requested_contracts, filled_contracts=filled,
                average_fill_price=average_price, partial_execution=True,
                unfilled_remainder_cancelled=True, authoritative_cancellation=True,
                protection=protection.__dict__, recovery_required=True,
                recovery_reason="partial_cancel_registry_protection_status_write_pending",
                position_registry_error=f"{type(exc).__name__}:{exc}",
            )
        evidence["protection"] = protection.__dict__
        return await self._finalize_terminal(
            request, state="CANCELLED", terminal_class="cancelled",
            reason="partial_execution_remainder_cancelled", exchange_contacted=True, fields=evidence,
        )

    async def cancel(self, coin: str, *, order_id: str = "", client_order_id: str = "", intent_id: str = "") -> dict:
        # A caller without an intent has no durable idempotency identity.  It
        # remains a compatibility path; all router-owned cancellations take
        # the serialized boundary below.
        if not intent_id:
            return await self.client.cancel_order(
                f"{coin.upper()}-USDT-SWAP", order_id, client_order_id=client_order_id,
            )
        async with self._lock(intent_id):
            existing = self.orders.get(intent_id)
            if not existing:
                return {"status": "CANCEL_REJECTED", "reason": "cancel_intent_missing"}
            if existing.get("terminal_finalized_at"):
                return {
                    "status": "CANCEL_ALREADY_RESOLVED", "intent_id": intent_id,
                    "state": str(existing.get("state", "")), "post_count": 0,
                }
            prepare = getattr(self.orders, "prepare_cancel_boundary", None)
            if callable(prepare):
                existing, should_post = prepare(
                    intent_id, order_id=order_id, client_order_id=client_order_id,
                )
            else:
                # Compatibility only for injected legacy stores.  Production
                # OrderStateStore provides the durable boundary above.
                self.orders.transition(intent_id, "CANCEL_PENDING")
                should_post = True
            if not should_post:
                return {
                    "status": "CANCEL_IN_PROGRESS", "intent_id": intent_id,
                    "state": str(existing.get("state", "CANCEL_PENDING")), "post_count": 0,
                }
            response = await self.client.cancel_order(
                f"{coin.upper()}-USDT-SWAP", order_id, client_order_id=client_order_id,
            )
            request = self._request_from_row(self.orders.get(intent_id) or existing)
            # OKX has only acknowledged the cancellation request.  Read back
            # once (bounded; no polling loop) and retain CANCEL_PENDING when
            # final state truth is not yet available.
            try:
                status = await self.client.get_order(
                    request.coin, order_id=order_id, client_order_id=client_order_id,
                )
            except Exception as exc:
                status = {}
                self.orders.transition(
                    intent_id, "CANCEL_PENDING", cancel_response=response,
                    recovery_required=True,
                    recovery_reason=f"cancel_confirmation_query_failed:{type(exc).__name__}:{exc}",
                )
            state = str((status or {}).get("state", "") or "").lower()
            if state in {"canceled", "cancelled", "expired", "mmp_canceled"}:
                await self._converge_authoritative_partial_cancel(request, status, response, order_id)
                self._reconcile_authoritative_zero_fill_risk_slot(intent_id)
                try:
                    from capital_scheduler import scheduler
                    scheduler.notify("order_cancelled")
                except Exception:
                    pass
            elif state:
                self.orders.transition(
                    intent_id, "CANCEL_PENDING", cancel_response=response,
                    cancel_confirmation=status, recovery_required=True,
                    recovery_reason=f"cancel_not_terminal:{state}",
                )
            elif not status:
                self.orders.transition(
                    intent_id, "CANCEL_PENDING", cancel_response=response,
                    recovery_required=True, recovery_reason="cancel_confirmation_unresolved",
                )
            return response

    async def _operator_position_truth(self, coin: str) -> dict:
        """Read the current exchange position before an operator mutation."""
        reader = getattr(self.client, "get_positions_status", None)
        try:
            if callable(reader):
                try:
                    result = await reader(force_refresh=True, priority="P0")
                except TypeError:
                    try:
                        result = await reader(force_refresh=True)
                    except TypeError:
                        result = await reader()
            else:
                rows = await self.client.get_positions()
                result = {"ok": True, "positions": rows}
        except Exception as exc:
            return {"ok": False, "reason": f"position_truth_unavailable:{exc}", "positions": None}
        if not isinstance(result, dict) or not bool(result.get("ok", False)):
            return {
                "ok": False,
                "reason": str((result or {}).get("reason", "position_truth_unavailable")),
                "positions": None,
            }
        target = str(coin or "").upper()
        matches = []
        for row in list(result.get("positions", []) or []):
            inst_id = str(row.get("instId", "") or "").upper()
            row_coin = inst_id.replace("-USDT-SWAP", "").replace("-USDT", "")
            if row_coin != target:
                continue
            try:
                quantity = abs(float(row.get("pos", row.get("availPos", row.get("posSize", 0.0))) or 0.0))
            except Exception:
                quantity = 0.0
            if quantity > 0:
                matches.append((quantity, dict(row)))
        if not matches:
            return {"ok": True, "positions": list(result.get("positions", []) or []), "position": None, "contracts": 0.0}
        quantity, row = max(matches, key=lambda item: item[0])
        pos_side = str(row.get("posSide", "") or "").lower()
        if pos_side not in {"long", "short"}:
            try:
                signed = float(row.get("pos", 0.0) or 0.0)
            except Exception:
                signed = 0.0
            pos_side = "long" if signed > 0 else "short" if signed < 0 else ""
        return {
            "ok": True,
            "positions": list(result.get("positions", []) or []),
            "position": row,
            "contracts": quantity,
            "pos_side": pos_side,
        }

    async def _operator_position_mode(self) -> dict:
        """Read authoritative account position mode before a close POST."""
        reader = getattr(self.client, "get_account_config", None)
        if not callable(reader):
            return {"ok": False, "reason": "position_mode_truth_reader_missing"}
        try:
            try:
                config = await reader(priority="P0")
            except TypeError:
                config = await reader()
        except Exception as exc:
            return {"ok": False, "reason": f"position_mode_truth_unavailable:{exc}"}
        mode = str((config or {}).get("posMode", "") or "").lower()
        if mode not in {"net_mode", "long_short_mode"}:
            return {"ok": False, "reason": f"position_mode_unknown:{mode or 'missing'}"}
        return {"ok": True, "position_mode": mode}

    async def _operator_instrument(self, coin: str):
        """Resolve shared authoritative instrument metadata for a close."""
        getter = getattr(self.instruments, "get", None)
        if callable(getter):
            item = getter(coin, require_fresh=True)
            if item is not None:
                eligible = getattr(item, "eligible", None)
                if not callable(eligible) or bool(eligible()[0]):
                    return item
        refresh = getattr(self.instruments, "refresh", None)
        if callable(refresh):
            try:
                await refresh(self.client, force=True, priority="P0")
            except TypeError:
                await refresh(self.client, force=True)
        require = getattr(self.instruments, "require", None)
        if not callable(require):
            raise LookupError(f"missing_or_stale_instrument_metadata:{str(coin).upper()}")
        return require(coin)

    @staticmethod
    def _operator_exchange_lifecycle_id(position: dict | None = None) -> str:
        """Compatibility helper returning exchange recognition evidence."""
        try:
            from position_registry import derive_exchange_lifecycle_anchor
            return str(derive_exchange_lifecycle_anchor(position) or "")
        except Exception:
            return ""

    @classmethod
    def _operator_lifecycle_context(cls, coin: str, position: dict | None = None) -> dict:
        """Resolve the persisted lifecycle and exchange recognition anchor."""
        try:
            from position_registry import (
                derive_position_lifecycle_id,
                get_all_details,
                resolve_position_lifecycle,
            )
            row = dict(get_all_details().get(str(coin).upper(), {}) or {})
            resolution = dict(resolve_position_lifecycle(row, position) or {})
            resolution["local_id"] = str(derive_position_lifecycle_id(row) or "")
            resolution["exchange_id"] = str(resolution.get("exchange_lifecycle_anchor", "") or "")
            resolution["current_id"] = str(resolution.get("position_lifecycle_id", "") or "")
            resolution["validated"] = resolution.get("status") in {"MATCHED", "BOUND", "STALE_LOCAL", "NEW_LIFECYCLE"}
            resolution["mismatch"] = resolution.get("status") == "STALE_LOCAL"
            return resolution
        except Exception:
            return {
                "status": "UNRESOLVED", "local_id": "", "exchange_id": "",
                "current_id": "", "validated": False, "mismatch": False,
            }

    @classmethod
    def _operator_lifecycle_id(cls, coin: str, position: dict | None = None) -> str:
        """Compatibility wrapper returning only a validated exchange identity."""
        return str(cls._operator_lifecycle_context(coin, position).get("current_id", "") or "")

    async def operator_close(
        self, coin: str, *, requested_pos_side: str = "", source: str = "operator",
        requested_contracts: float = 0.0, intent_id: str = "",
    ) -> dict:
        """Durable, exchange-truth-first operator close pipeline.

        This is intentionally separate from strategy exits: operator intent
        bypasses strategy qualification, but never bypasses exchange truth,
        metadata validation, durable persistence, reduce-only sizing, or the
        one-POST mutation boundary.
        """
        coin = str(coin or "").upper()
        async with self._lock(f"operator-close:{coin}"):
            truth = await self._operator_position_truth(coin)
            if not truth.get("ok"):
                return {"status": "CLOSE_FAILED_BEFORE_SUBMISSION", "reason": truth.get("reason", "position_truth_unavailable"), "post_count": 0}
            position = truth.get("position")
            exchange_contracts = float(truth.get("contracts", 0.0) or 0.0)
            if position is None or exchange_contracts <= 0:
                if not self._reconcile_closed_position_risk_slot(coin):
                    return {"status": "CLOSE_UNCERTAIN", "reason": "position_risk_slot_close_pending", "post_count": 0}
                return {"status": "CLOSE_ALREADY_ZERO", "reason": "exchange_position_zero", "post_count": 0}

            pos_side = str(truth.get("pos_side", "") or requested_pos_side or "").lower()
            if pos_side not in {"long", "short"}:
                return {"status": "CLOSE_FAILED_BEFORE_SUBMISSION", "reason": "exchange_position_side_unknown", "post_count": 0}
            requested_contracts = float(requested_contracts or 0.0)
            if requested_contracts < 0:
                return {"status": "CLOSE_FAILED_BEFORE_SUBMISSION", "reason": "close_contracts_negative", "post_count": 0}
            if requested_contracts > exchange_contracts + 1e-9:
                return {
                    "status": "CLOSE_FAILED_BEFORE_SUBMISSION",
                    "reason": "close_contracts_exceed_authoritative_position",
                    "post_count": 0,
                }
            contracts = requested_contracts if requested_contracts > 0 else exchange_contracts

            mode_truth = await self._operator_position_mode()
            if not mode_truth.get("ok"):
                return {"status": "CLOSE_FAILED_BEFORE_SUBMISSION", "reason": mode_truth.get("reason", "position_mode_unknown"), "post_count": 0}
            position_mode = str(mode_truth["position_mode"])

            # A fresh exchange position resolves an otherwise unknown local
            # row before any operator mutation is authorized. This is a local
            # truth update, never an exchange-side assumption.
            try:
                from position_registry import (
                    get_all_details as _registry_details,
                    is_unresolved_exposure as _registry_unknown,
                    update as _registry_update,
                )
                local_row = dict(_registry_details().get(coin, {}) or {})
                if local_row and _registry_unknown(local_row):
                    _registry_update(
                        coin,
                        metadata={
                            "exposure_truth_status": "ACTIVE_POSITION",
                            "confirmed_remaining_contracts": exchange_contracts,
                            "exchange_position_timestamp": datetime.now(timezone.utc).isoformat(),
                            "sync_status": "OK",
                            "last_seen_on_exchange": datetime.now(timezone.utc).isoformat(),
                            "resolved_from_local_unknown": True,
                        },
                    )
            except Exception:
                logger.exception("Failed to persist fresh exchange truth for operator close %s", coin)

            lifecycle = self._operator_lifecycle_context(coin, position)
            lifecycle_id = str(lifecycle.get("current_id", "") or "")
            lifecycle_status = str(lifecycle.get("status", "UNRESOLVED") or "UNRESOLVED")
            exchange_anchor = str(lifecycle.get("exchange_lifecycle_anchor", "") or "")
            if not lifecycle_id or not exchange_anchor or lifecycle_status == "UNRESOLVED":
                return {
                    "status": "CLOSE_FAILED_BEFORE_SUBMISSION",
                    "reason": str(lifecycle.get("reason", "position_lifecycle_identity_unavailable")),
                    "post_count": 0,
                }
            try:
                from position_registry import (
                    adopt_exchange_position as _adopt_exchange_position,
                    get_all_details as _registry_details,
                    update as _registry_update,
                )
                current_local = dict(_registry_details().get(coin, {}) or {})
                local_id = str(lifecycle.get("local_id", "") or "")
                if current_local:
                    _registry_update(
                        coin,
                        metadata={
                            "position_lifecycle_id": lifecycle_id,
                            "exchange_lifecycle_anchor": exchange_anchor,
                            "lifecycle_resolution_status": lifecycle_status,
                            "lifecycle_identity_source": "canonical_position_lifecycle_resolver",
                            "previous_position_lifecycle_id": local_id if local_id != lifecycle_id else "",
                        },
                        require_persist=True,
                    )
                elif lifecycle_status == "NEW_LIFECYCLE":
                    if not _adopt_exchange_position(
                        coin,
                        position,
                        position_lifecycle_id=lifecycle_id,
                        exchange_lifecycle_anchor=exchange_anchor,
                    ):
                        raise OSError("exchange_lifecycle_adoption_persistence_failed")
            except Exception as exc:
                return {
                    "status": "CLOSE_FAILED_BEFORE_SUBMISSION",
                    "reason": f"position_lifecycle_persistence_failed:{exc}",
                    "post_count": 0,
                }

            existing = self.orders.find_active(
                kind="manual_close", coin=coin, pos_side=pos_side,
                position_lifecycle_id=lifecycle_id,
            )
            if existing:
                # The no-POST-before-durability callback makes a surviving
                # PAYLOAD_READY manual-close row proof that no close POST was
                # attempted.  This also safely converges a prior boundary
                # persistence failure once storage returns, without a retry
                # mutation.
                if (
                    str(existing.get("state", "") or "") == "PAYLOAD_READY"
                    and not bool(existing.get("mutation_attempted", False))
                ):
                    existing_request = self._request_from_row(existing)
                    try:
                        final = await self._finalize_terminal(
                            existing_request, state="FAILED_FINAL", terminal_class="local_rejection",
                            reason="manual_close_pre_submission_persistence_failure",
                            exchange_contacted=False,
                            fields={
                                "manual_close": True, "manual_close_source": source,
                                "mutation_outcome": "PRE_SUBMISSION_PERSISTENCE_FAILURE",
                                "mutation_attempted": False, "physical_post_count": 0,
                                "submission_boundary_evidence": {
                                    "post_not_attempted": True,
                                    "durable_state_before_convergence": "PAYLOAD_READY",
                                },
                            },
                        )
                    except Exception as exc:
                        return {
                            "status": "CLOSE_PERSISTENCE_PENDING", "reason": f"manual_close_terminal_persistence_failed:{exc}",
                            "intent_id": str(existing.get("intent_id", "")),
                            "client_order_id": str(existing.get("client_order_id", "")), "post_count": 0,
                        }
                    return {
                        "status": "CLOSE_FAILED_BEFORE_SUBMISSION",
                        "reason": str(final.get("terminal_reason", "manual_close_pre_submission_persistence_failure")),
                        "intent_id": str(existing.get("intent_id", "")),
                        "client_order_id": str(existing.get("client_order_id", "")), "post_count": 0,
                    }
                return {
                    "status": "CLOSE_IN_PROGRESS",
                    "reason": "active_manual_close_intent",
                    "intent_id": str(existing.get("intent_id", "")),
                    "client_order_id": str(existing.get("client_order_id", "")),
                    "state": str(existing.get("state", "")),
                    "post_count": int(existing.get("physical_post_count", 0) or 0),
                }

            try:
                instrument = await self._operator_instrument(coin)
            except Exception as exc:
                return {"status": "CLOSE_FAILED_BEFORE_SUBMISSION", "reason": str(exc), "post_count": 0}

            intent_id = str(intent_id or f"MANUAL-CLOSE-{coin}-{uuid.uuid4().hex[:16].upper()}")
            client_order_id = deterministic_client_order_id(intent_id, "C")
            position_inst_id = str(position.get("instId", "") or getattr(instrument, "inst_id", ""))
            margin_mode = str(position.get("mgnMode", position.get("tdMode", "isolated")) or "isolated").lower()
            row = {
                "intent_id": intent_id,
                "kind": "manual_close",
                "source": source,
                "coin": coin,
                "pos_side": pos_side,
                "direction": pos_side,
                "inst_id": position_inst_id,
                "position_lifecycle_id": lifecycle_id,
                "exchange_lifecycle_anchor": exchange_anchor,
                "lifecycle_resolution_status": lifecycle_status,
                "position_mode": position_mode,
                "requested_contracts": contracts,
                "approved_contracts": contracts,
                "exchange_contracts_at_validation": contracts,
                "reduce_only": True,
                "margin_mode": margin_mode,
                "client_order_id": client_order_id,
                "physical_post_count": 0,
                "mutation_attempted": False,
                "metadata_source": "authoritative_instrument_registry",
            }
            try:
                self.orders.create(row)
                self.orders.transition(intent_id, "ROUTER_VALIDATING", position_truth=dict(position), metadata=instrument.raw if hasattr(instrument, "raw") else {})
                self.orders.transition(intent_id, "PAYLOAD_READY", payload_contracts=contracts, reduce_only=True)
            except Exception as exc:
                logger.error("Manual close intent persistence failed for %s: %s", coin, exc)
                return {"status": "CLOSE_FAILED_BEFORE_SUBMISSION", "reason": f"intent_persistence_failed:{exc}", "post_count": 0}

            # Manual close has its own authority path, but its terminal
            # accounting is still router-owned and must use the canonical
            # durable finalizer/telemetry path.
            manual_request = ExecutionRequest(
                intent_id=intent_id, candidate_id="", reservation_id="",
                mode="manual_close", engine=str(source or "operator"), coin=coin,
                direction=pos_side, requested_contracts=contracts,
                approved_contracts=contracts, approved_notional_usdt=0.0,
                order_type="market", reduce_only=True, margin_mode=margin_mode,
                metadata={"manual_close": True, "source": source},
            )

            submission_boundary_persisted = False

            def mark_submission_attempted() -> None:
                nonlocal submission_boundary_persisted
                if submission_boundary_persisted:
                    return
                try:
                    # This is the exact mutation boundary: OKXClient invokes
                    # it only after final truth/payload validation and REST
                    # admission, immediately before the HTTP request.
                    self.orders.transition(
                        intent_id, "SUBMISSION_ATTEMPTED", mutation_attempted=True,
                        exchange_contacted=True, physical_post_count=1,
                    )
                except Exception as exc:
                    raise ManualCloseSubmissionBoundaryError(
                        f"manual_close_submission_boundary_persistence_failed:{exc}"
                    ) from exc
                submission_boundary_persisted = True

            try:
                response = await self.client.close_futures_position(
                    coin, pos_side, 0.0, contracts_override=contracts,
                    client_order_id=client_order_id, instrument_metadata=instrument,
                    margin_mode=margin_mode, position_mode=position_mode,
                    verify_authoritative_position=True,
                    before_send=mark_submission_attempted,
                )
                response = dict(response or {})
                client_outcome = str(response.get("mutation_outcome", "") or "")
                if client_outcome in {MUTATION_SUCCESS, MUTATION_DEFINITIVE_REJECTION, MUTATION_READBACK_REQUIRED}:
                    evidence = dict(response.get("mutation_evidence", {}) or {})
                    classification = MutationClassification(
                        client_outcome,
                        client_id=client_order_id,
                        exchange_order_id=str(response.get("exchange_order_id", response.get("ordId", "")) or ""),
                        exchange_id=str(response.get("exchange_order_id", response.get("ordId", "")) or ""),
                        evidence=evidence,
                    )
                else:
                    classification = classify_mutation_response(
                        response, expected_items=1, submitted_client_id=client_order_id,
                    )
            except Exception as exc:
                if isinstance(exc, ManualCloseSubmissionBoundaryError):
                    try:
                        final = await self._finalize_terminal(
                            manual_request, state="FAILED_FINAL", terminal_class="local_rejection",
                            reason="manual_close_pre_submission_persistence_failure",
                            exchange_contacted=False,
                            fields={
                                "submit_error": str(exc),
                                "mutation_outcome": "PRE_SUBMISSION_PERSISTENCE_FAILURE",
                                "mutation_attempted": False, "physical_post_count": 0,
                                "manual_close": True, "manual_close_source": source,
                                "submission_boundary_evidence": {
                                    "post_not_attempted": True,
                                    "failure": str(exc),
                                },
                            },
                        )
                    except Exception as finalization_error:
                        durable = self.orders.get(intent_id) or {}
                        if not durable.get("terminal_finalized_at"):
                            return {
                                "status": "CLOSE_PERSISTENCE_PENDING",
                                "reason": f"manual_close_terminal_persistence_failed:{finalization_error}",
                                "intent_id": intent_id, "client_order_id": client_order_id,
                                "post_count": 0,
                            }
                        final = durable
                    return {
                        "status": "CLOSE_FAILED_BEFORE_SUBMISSION",
                        "reason": str(final.get("terminal_reason", "manual_close_pre_submission_persistence_failure")),
                        "intent_id": intent_id, "client_order_id": client_order_id,
                        "post_count": 0,
                    }
                if exc.__class__.__name__ == "OKXDefinitiveRejection":
                    evidence = dict(getattr(exc, "evidence", {}) or {})
                    rejection_readback = await self._operator_position_truth(coin)
                    await self._finalize_terminal(
                        manual_request, state="FAILED_FINAL", terminal_class="exchange_rejection",
                        reason="manual_close_exchange_rejected", exchange_contacted=True,
                        fields={"mutation_outcome": MUTATION_DEFINITIVE_REJECTION,
                                "rejection_reason": str(exc), "mutation_evidence": evidence,
                                "recovery_status": rejection_readback, "manual_close": True,
                                "manual_close_source": source},
                    )
                    return {"status": "CLOSE_REJECTED", "reason": str(exc), "intent_id": intent_id, "client_order_id": client_order_id, "post_count": 1}
                # A durable boundary is stronger evidence than the Python
                # exception class.  Parsing/readback failures after it may
                # follow a physical POST and must retain read-only recovery
                # ownership rather than manufacture a zero-POST terminal.
                if submission_boundary_persisted:
                    self.orders.transition(
                        intent_id, "SUBMIT_UNCERTAIN", submit_error=str(exc),
                        mutation_attempted=True, physical_post_count=1,
                        exchange_contacted=True, mutation_outcome="POST_BOUNDARY_UNCERTAIN",
                    )
                    return {"status": "CLOSE_UNCERTAIN", "reason": str(exc), "intent_id": intent_id, "client_order_id": client_order_id, "post_count": 1}
                transport = {"Timeout", "ConnectionError", "ConnectTimeout", "ReadTimeout", "PoolTimeout"}
                if "close_position_truth_unavailable" in str(exc):
                    await self._finalize_terminal(
                        manual_request, state="FAILED_FINAL", terminal_class="local_rejection",
                        reason="manual_close_pre_submission_failure", exchange_contacted=False,
                        fields={"submit_error": str(exc), "mutation_outcome": "PRE_SUBMISSION_FAILURE",
                                "mutation_attempted": False, "physical_post_count": 0,
                                "manual_close": True, "manual_close_source": source},
                    )
                    return {"status": "CLOSE_FAILED_BEFORE_SUBMISSION", "reason": str(exc), "intent_id": intent_id, "client_order_id": client_order_id, "post_count": 0}
                if isinstance(exc, (ValueError, LookupError)):
                    await self._finalize_terminal(
                        manual_request, state="FAILED_FINAL", terminal_class="local_rejection",
                        reason="manual_close_pre_submission_failure", exchange_contacted=False,
                        fields={"submit_error": str(exc), "mutation_outcome": "PRE_SUBMISSION_FAILURE",
                                "mutation_attempted": False, "physical_post_count": 0,
                                "manual_close": True, "manual_close_source": source},
                    )
                    return {"status": "CLOSE_FAILED_BEFORE_SUBMISSION", "reason": str(exc), "intent_id": intent_id, "client_order_id": client_order_id, "post_count": 0}
                if any(token in exc.__class__.__name__ for token in transport):
                    self.orders.transition(intent_id, "SUBMIT_UNCERTAIN", submit_error=str(exc), mutation_outcome="TRANSPORT_UNCERTAIN")
                    return {"status": "CLOSE_UNCERTAIN", "reason": str(exc), "intent_id": intent_id, "client_order_id": client_order_id, "post_count": 1}
                await self._finalize_terminal(
                    manual_request, state="FAILED_FINAL", terminal_class="local_rejection",
                    reason="manual_close_pre_submission_failure", exchange_contacted=False,
                    fields={"submit_error": str(exc), "mutation_outcome": "PRE_SUBMISSION_FAILURE",
                            "mutation_attempted": False, "physical_post_count": 0,
                            "manual_close": True, "manual_close_source": source},
                )
                return {"status": "CLOSE_FAILED_BEFORE_SUBMISSION", "reason": str(exc), "intent_id": intent_id, "client_order_id": client_order_id, "post_count": 0}

            evidence = classification.to_dict()
            post_attempted = bool(response.get("post_attempted", True))
            physical_post_count = 1 if post_attempted else 0
            exchange_order_id = (
                (classification.exchange_order_id or str(response.get("ordId", "") or ""))
                if post_attempted else ""
            )
            if not post_attempted:
                self.orders.transition(
                    intent_id, "RECOVERY_QUERY", mutation_attempted=False,
                    physical_post_count=0, exchange_contacted=False,
                )
            self.orders.transition(
                intent_id, "SUBMITTED" if post_attempted and classification.outcome == MUTATION_SUCCESS else "RECOVERY_QUERY",
                response=response, mutation_outcome=classification.outcome,
                mutation_evidence=evidence, exchange_order_id=exchange_order_id,
                client_order_id=client_order_id,
            )
            readback = await self._operator_position_truth(coin)
            remaining = float(readback.get("contracts", 0.0) or 0.0) if readback.get("ok") else None
            if not post_attempted:
                if readback.get("ok") and remaining == 0.0:
                    # Even though this close helper reports no physical POST,
                    # authoritative zero still closes the original entry's
                    # risk slot.  That local close is durable/idempotent; a
                    # persistence failure leaves this manual intent
                    # recovery-owned rather than falsely terminalizing it.
                    if not self._reconcile_closed_position_risk_slot(coin):
                        self.orders.transition(
                            intent_id, "SUBMIT_UNCERTAIN", recovery_status=readback,
                            mutation_evidence=evidence, recovery_required=True,
                            recovery_reason="manual_close_position_risk_slot_close_pending",
                            mutation_attempted=False, physical_post_count=0,
                        )
                        return {
                            "status": "CLOSE_UNCERTAIN", "intent_id": intent_id,
                            "client_order_id": client_order_id,
                            "reason": "position_risk_slot_close_pending", "post_count": 0,
                        }
                    await self._finalize_terminal(
                        manual_request, state="FAILED_FINAL", terminal_class="reconciliation_terminal",
                        reason="manual_close_position_zero_before_submission", exchange_contacted=False,
                        fields={"mutation_attempted": False, "physical_post_count": 0,
                                "mutation_outcome": "ALREADY_ZERO", "close_outcome": "CLOSE_ALREADY_ZERO",
                                "recovery_status": readback, "mutation_evidence": evidence,
                                "exchange_closed": True, "manual_close": True,
                                "manual_close_source": source},
                    )
                    return {
                        "status": "CLOSE_ALREADY_ZERO",
                        "intent_id": intent_id,
                        "client_order_id": client_order_id,
                        "reason": "exchange_position_zero_before_submission",
                        "response": response,
                        "post_count": 0,
                    }
                await self._finalize_terminal(
                    manual_request, state="FAILED_FINAL", terminal_class="local_rejection",
                    reason="manual_close_position_recheck_not_zero_or_unavailable", exchange_contacted=False,
                    fields={"mutation_attempted": False, "physical_post_count": 0,
                            "mutation_outcome": "PRE_SUBMISSION_FAILURE",
                            "close_outcome": "FAILED_BEFORE_SUBMISSION",
                            "recovery_status": readback, "mutation_evidence": evidence,
                            "manual_close": True, "manual_close_source": source},
                )
                return {
                    "status": "CLOSE_FAILED_BEFORE_SUBMISSION",
                    "intent_id": intent_id,
                    "client_order_id": client_order_id,
                    "reason": "position_recheck_not_zero_or_unavailable",
                    "response": response,
                    "post_count": 0,
                }
            if classification.outcome == MUTATION_DEFINITIVE_REJECTION:
                await self._finalize_terminal(
                    manual_request, state="FAILED_FINAL", terminal_class="exchange_rejection",
                    reason="manual_close_exchange_rejected", exchange_contacted=True,
                    fields={"mutation_evidence": evidence, "recovery_status": readback,
                            "mutation_outcome": MUTATION_DEFINITIVE_REJECTION,
                            "manual_close": True, "manual_close_source": source},
                )
                return {"status": "CLOSE_REJECTED", "intent_id": intent_id, "client_order_id": client_order_id, "reason": classification.item_s_msg or classification.top_level_msg, "post_count": physical_post_count}
            if readback.get("ok") and remaining == 0.0:
                if not self._reconcile_closed_position_risk_slot(coin):
                    self.orders.transition(
                        intent_id, "SUBMIT_UNCERTAIN", recovery_status=readback,
                        mutation_evidence=evidence, recovery_required=True,
                        recovery_reason="manual_close_position_risk_slot_close_pending",
                    )
                    return {"status": "CLOSE_UNCERTAIN", "intent_id": intent_id, "client_order_id": client_order_id, "exchange_order_id": exchange_order_id, "reason": "position_risk_slot_close_pending", "post_count": physical_post_count}
                await self._finalize_terminal(
                    manual_request, state="FILLED", terminal_class="filled",
                    reason="manual_close_authoritatively_confirmed",
                    exchange_contacted=True,
                    fields={
                        "filled_contracts": contracts, "exchange_closed": True,
                        "exchange_order_id": exchange_order_id, "response": response,
                        "manual_close": True, "manual_close_source": source,
                        "recovery_status": readback, "mutation_evidence": evidence,
                    },
                )
                return {"status": "CLOSE_FILLED", "intent_id": intent_id, "client_order_id": client_order_id, "exchange_order_id": exchange_order_id, "response": response, "post_count": physical_post_count}
            if classification.outcome == MUTATION_READBACK_REQUIRED or not readback.get("ok"):
                self.orders.transition(intent_id, "SUBMIT_UNCERTAIN", recovery_status=readback, mutation_evidence=evidence)
                return {"status": "CLOSE_UNCERTAIN", "intent_id": intent_id, "client_order_id": client_order_id, "exchange_order_id": exchange_order_id, "reason": "exchange_close_requires_readback", "post_count": physical_post_count}
            self.orders.transition(intent_id, "ACKNOWLEDGED", remaining_contracts=remaining or 0.0)
            return {"status": "CLOSE_SUBMITTED", "intent_id": intent_id, "client_order_id": client_order_id, "exchange_order_id": exchange_order_id, "remaining_contracts": remaining or 0.0, "response": response, "post_count": physical_post_count}

    async def close(
        self, coin: str, pos_side: str, size_usdt: float, *, contracts: float = 0.0,
        partial: bool = False, emergency: bool = False, intent_id: str = "",
    ) -> dict | None:
        """Compatibility adapter to the durable close lifecycle.

        Strategy, exit, and protection callers may retain this interface, but
        no longer receive a direct exchange-close bypass.  The supplied
        contract quantity is treated as a requested maximum and is validated
        against authoritative live position truth inside ``operator_close``.
        """
        close_intent = intent_id or f"CLOSE-{coin.upper()}-{pos_side}-{uuid.uuid4().hex[:12]}"
        close_contracts = float(contracts or 0.0)
        if partial and not self.paper_mode() and close_contracts <= 0 and float(size_usdt or 0.0) > 0:
            instrument = self.instruments.require(coin)
            ticker_getter = getattr(self.client, "get_execution_ticker", None)
            ticker = await (ticker_getter(coin) if ticker_getter else self.client.get_ticker(coin))
            price = float(ticker.get("last", 0.0) or 0.0)
            close_contracts = instrument.notional_to_contracts(float(size_usdt), price)
            feasible, reason = instrument.quantity_feasible(close_contracts, "market")
            if not feasible:
                raise ValueError(f"close_quantity_incompatible:{reason}")
        result = await self.operator_close(
            coin,
            requested_pos_side=pos_side,
            source="emergency_strategy_close" if emergency else ("strategy_partial_close" if partial else "strategy_close"),
            requested_contracts=close_contracts,
            intent_id=close_intent,
        )
        # Preserve the legacy response shape without hiding lifecycle status.
        response = dict(result.get("response", {}) or {})
        response.update({key: value for key, value in result.items() if key not in {"response"}})
        response.setdefault("execution_intent_id", close_intent)
        response.setdefault("requested_contracts", close_contracts)
        return response

    def _result_from_row(self, row: dict, reason: str, ok: bool | None = None) -> ExecutionResult:
        state = str(row.get("state", ""))
        return ExecutionResult(
            ok=state in {"FILLED", "PARTIALLY_FILLED"} if ok is None else bool(ok),
            reason=reason,
            intent_id=str(row.get("intent_id", "")),
            client_order_id=str(row.get("client_order_id", "")),
            order_id=str(row.get("exchange_order_id", "")),
            state=state,
            filled_contracts=float(row.get("filled_contracts", 0.0) or 0.0),
            average_fill_price=float(row.get("average_fill_price", 0.0) or 0.0),
            response=dict(row.get("response", {}) or {}),
            reservation_id=str(row.get("reservation_id", "")),
            protection=dict(row.get("protection", {}) or {}),
            requested_leverage=(int(row.get("requested_leverage")) if row.get("requested_leverage") not in (None, "") else None),
            exchange_configured_leverage=(int(row.get("exchange_configured_leverage")) if row.get("exchange_configured_leverage") not in (None, "") else None),
            position_effective_leverage=(int(row.get("position_effective_leverage")) if row.get("position_effective_leverage") not in (None, "") else None),
            leverage_truth_status=str(row.get("leverage_truth_status", "") or ""),
            leverage_truth_timestamp=str(row.get("leverage_truth_timestamp", "") or ""),
            leverage_truth_source=str(row.get("leverage_truth_source", "") or ""),
            leverage_truth_mismatch=bool(row.get("leverage_truth_mismatch", False)),
            requested_vs_configured_mismatch=bool(row.get("requested_vs_configured_mismatch", False)),
            configured_vs_effective_mismatch=bool(row.get("configured_vs_effective_mismatch", False)),
        )


def new_intent_id(candidate_id: str, engine: str, coin: str, direction: str) -> str:
    return hashlib.sha256(f"{candidate_id}|{engine}|{coin}|{direction}".encode("utf-8")).hexdigest()[:32]


_routers: dict[int, ExecutionRouter] = {}


def router_for(client) -> ExecutionRouter:
    return _routers.setdefault(id(client), ExecutionRouter(client))

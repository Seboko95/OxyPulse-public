"""Exchange-side protection lifecycle for confirmed live fills."""
from __future__ import annotations

import asyncio
import hashlib
import logging
import time
from dataclasses import dataclass, field
from decimal import Decimal, ROUND_HALF_UP
from datetime import datetime, timezone

import market_mode as mm
from instrument_registry import registry as _instrument_registry
from tp_ownership import (
    LOCAL_STAGED,
    EXCHANGE_FULL,
    NO_PROFIT_TP,
    VALID_TP_OWNERS,
    effective_exchange_take_profit,
    normalize_persisted_owner,
)
from okx_client_ids import (
    InvalidOKXClientID,
    canonical_protection_algo_client_id,
    validate_client_id,
)
from okx_mutation_results import (
    MUTATION_DEFINITIVE_REJECTION,
    MUTATION_READBACK_REQUIRED,
    MUTATION_SUCCESS,
    classify_mutation_response,
)

logger = logging.getLogger("OxyPulse.Protection")

_LIVE_PROTECTION_STATES = {"live", "effective"}
PROTECTED = "PROTECTED"
CONFIRMED_MISSING = "CONFIRMED_MISSING"
PROTECTION_STATE_UNKNOWN = "PROTECTION_STATE_UNKNOWN"
_CONFIRMATION_DELAY_SEC = 0.10
REPROTECTION_CONFIRMED = "REPROTECTION_CONFIRMED"
REPROTECTION_UNCHANGED = "REPROTECTION_UNCHANGED"
REPROTECTION_PENDING = "REPROTECTION_PENDING"
REPROTECTION_UNKNOWN = "REPROTECTION_UNKNOWN"
REPROTECTION_FAILED = "REPROTECTION_FAILED"
PROTECTION_INTENT_RESERVED = "PROTECTION_INTENT_RESERVED"
PROTECTION_SUBMISSION_ATTEMPTED = "PROTECTION_SUBMISSION_ATTEMPTED"
PROTECTION_CONFIRMED = "PROTECTION_CONFIRMED"
PROTECTION_SUBMIT_UNCERTAIN = "PROTECTION_SUBMIT_UNCERTAIN"
PROTECTION_CONFIRMED_NOT_FOUND = "PROTECTION_CONFIRMED_NOT_FOUND"
PROTECTION_CONFIRMED_CLEANUP_PENDING = "PROTECTION_CONFIRMED_CLEANUP_PENDING"


def _close_side(direction: str) -> str:
    return "sell" if direction.lower() in {"long", "buy"} else "buy"


def _is_reducing(row: dict) -> bool:
    if str(row.get("reduceOnly", "")).lower() in {"true", "1", "yes"}:
        return True
    # Some OKX close-all representations use closeFraction instead of an
    # explicit reduceOnly flag.  Accept only a non-empty fraction as reducing.
    return bool(str(row.get("closeFraction", "") or "").strip())


def _same_number(left: object, right: float, *, rel: float = 1e-6) -> bool:
    try:
        left_value = float(left)
        right_value = float(right)
    except (TypeError, ValueError):
        return False
    return abs(left_value - right_value) <= max(abs(right_value) * rel, 1e-9)


def _metadata_for(client, coin: str, instruments=None):
    for owner in (
        instruments,
        client,
        getattr(client, "instruments", None),
        getattr(client, "instrument_registry", None),
        getattr(client, "instrument_metadata", None),
    ):
        if owner is not None and hasattr(owner, "round_price") and hasattr(owner, "lot_sz"):
            return owner
        getter = getattr(owner, "get", None)
        if getter is None:
            continue
        try:
            item = getter(coin, require_fresh=True)
        except TypeError:
            item = getter(coin)
        except Exception:
            item = None
        if item is not None:
            return item
    try:
        return _instrument_registry.get(coin, require_fresh=True)
    except Exception:
        return None


def _normalized_price(value: object, metadata, *, position_side: str, purpose: str) -> float | None:
    try:
        price = float(value)
        if price <= 0:
            return None
        return float(metadata.round_price(price, side=position_side, purpose=purpose))
    except (TypeError, ValueError, AttributeError, ArithmeticError):
        return None


def _same_price(left: object, right: float, metadata, *, position_side: str, purpose: str) -> bool:
    left_normalized = _normalized_price(left, metadata, position_side=position_side, purpose=purpose)
    right_normalized = _normalized_price(right, metadata, position_side=position_side, purpose=purpose)
    return left_normalized is not None and right_normalized is not None and left_normalized == right_normalized


def _lot_units(value: object, lot_size: float) -> int | None:
    try:
        quantity = Decimal(str(value))
        lot = Decimal(str(lot_size))
    except (TypeError, ValueError, ArithmeticError):
        return None
    if quantity < 0 or lot <= 0:
        return None
    units = (quantity / lot).to_integral_value(rounding=ROUND_HALF_UP)
    # Serialization noise is harmless; a genuinely fractional lot is not.
    if abs((quantity / lot) - units) > Decimal("1e-9"):
        return None
    return int(units)


def _quantity_matches(row: dict, contracts: float, metadata=None) -> bool:
    if metadata is None:
        raw_size = row.get("sz", "")
        try:
            if raw_size not in {None, ""} and float(raw_size) > 0:
                return abs(float(raw_size) - float(contracts)) <= max(abs(float(contracts)) * 1e-8, 1e-9)
        except (TypeError, ValueError):
            return False
        fraction = str(row.get("closeFraction", "") or "").strip().rstrip("%").strip()
        try:
            return bool(fraction) and float(fraction) in {1.0, 100.0}
        except ValueError:
            return False
    lot_size = getattr(metadata, "lot_sz", 0.0)
    current_units = _lot_units(contracts, lot_size)
    if current_units is None:
        return False
    raw_size = row.get("sz", "")
    if raw_size not in {None, ""}:
        size_units = _lot_units(raw_size, lot_size)
        if size_units is not None and size_units == current_units:
            return True
        try:
            if float(raw_size) > 0:
                return False
        except (TypeError, ValueError):
            return False
    fraction = str(row.get("closeFraction", "") or "").strip().rstrip("%").strip()
    if not fraction:
        return False
    try:
        fraction_value = float(fraction)
    except ValueError:
        return False
    # A full-position close fraction is equivalent to an explicit size.  A
    # partial fraction cannot prove that the whole remaining position is
    # protected and therefore must not suppress a new protection order.
    return fraction_value in {1.0, 100.0}


def _protection_matches(
    row: dict,
    *,
    inst_id: str,
    pos_side: str,
    close_side: str,
    contracts: float,
    stop_price: float,
    take_profit: float,
    metadata,
) -> bool:
    legacy_view = bool(row.get("_legacy_protection_view"))
    if str(row.get("instId", "")).upper() != inst_id.upper():
        return False
    if legacy_view:
        # Compatibility only for older clients/test doubles that predate the
        # canonical merged view.  Real OKX rows are always strict below.
        if row.get("posSide") and str(row.get("posSide")).lower() != pos_side:
            return False
        if row.get("side") and str(row.get("side")).lower() != close_side:
            return False
        if "reduceOnly" in row and not _is_reducing(row):
            return False
    else:
        if str(row.get("posSide", "")).lower() != pos_side:
            return False
        if str(row.get("side", "")).lower() != close_side:
            return False
        if not _is_reducing(row):
            return False
    if not _quantity_matches(row, contracts, metadata):
        return False
    if metadata is None:
        if not legacy_view:
            return False
        if not _same_number(row.get("slTriggerPx"), stop_price):
            return False
    elif not _same_price(row.get("slTriggerPx"), stop_price, metadata, position_side=pos_side, purpose="stop"):
        return False
    if take_profit > 0:
        if metadata is None:
            if not legacy_view or not _same_number(row.get("tpTriggerPx"), take_profit):
                return False
        elif not _same_price(row.get("tpTriggerPx"), take_profit, metadata, position_side=pos_side, purpose="take_profit"):
            return False
    elif row.get("tpTriggerPx") not in {None, "", 0, 0.0, "0"}:
        return False
    return str(row.get("state", "live")).lower() in _LIVE_PROTECTION_STATES


@dataclass
class ProtectionResult:
    protected: bool
    reason: str
    sl_algo_id: str = ""
    tp_algo_ids: list[str] = field(default_factory=list)
    protected_contracts: float = 0.0
    emergency_closed: bool = False
    protection_state: str = ""
    confirmed_stop_price: float = 0.0
    desired_stop_price: float = 0.0
    superseded_algo_ids: list[str] = field(default_factory=list)
    error: str = ""
    intent_lifecycle: str = ""
    cleanup_pending: bool = False
    recovery_state: str = ""
    mutation_attempted: bool = False


class ProtectionManager:
    def __init__(
        self, client, instruments=None, *, strict_intent_persistence: bool = False,
        emergency_close=None,
    ):
        self.client = client
        self.instruments = instruments
        self.strict_intent_persistence = bool(strict_intent_persistence)
        self._emergency_close = emergency_close
        self._reprotect_locks: dict[tuple[str, str], asyncio.Lock] = {}

    def _reprotect_lock(self, coin: str, pos_side: str) -> asyncio.Lock:
        key = (str(coin or "").upper(), str(pos_side or "").lower())
        lock = self._reprotect_locks.get(key)
        if lock is None:
            lock = asyncio.Lock()
            self._reprotect_locks[key] = lock
        return lock

    @staticmethod
    def _live_scoped(view: dict, inst_id: str, pos_side: str) -> list[dict]:
        return [
            row for row in list(view.get("orders", []) or [])
            if str(row.get("state", "live")).lower() in _LIVE_PROTECTION_STATES
            and str(row.get("instId", "")).upper() == inst_id.upper()
            and (
                row.get("_legacy_protection_view")
                or str(row.get("posSide", "")).lower() == pos_side
            )
        ]

    @staticmethod
    def _stop_value(row: dict) -> float:
        try:
            return float(row.get("slTriggerPx", 0.0) or 0.0)
        except (TypeError, ValueError):
            return 0.0

    @staticmethod
    def _tp_value(row: dict) -> float:
        try:
            return float(row.get("tpTriggerPx", 0.0) or 0.0)
        except (TypeError, ValueError):
            return 0.0

    @staticmethod
    def _now_iso() -> str:
        return datetime.now(timezone.utc).isoformat()

    def _registry_row(self, coin: str) -> dict:
        try:
            from position_registry import get_all_details
            return dict((get_all_details() or {}).get(str(coin or "").upper(), {}) or {})
        except Exception:
            return {}

    def _position_lifecycle_id(
        self,
        coin: str,
        *,
        row: dict,
        prior_intent: dict,
        client_order_id: str,
        provided: str = "",
    ) -> str:
        """Resolve and persist immutable position identity before mutation."""
        identity = str(
            row.get("position_lifecycle_id", "")
            or prior_intent.get("position_lifecycle_id", "")
            or provided
            or ""
        ).strip()
        if not identity:
            try:
                from position_registry import derive_position_lifecycle_id
                identity = derive_position_lifecycle_id(row, client_order_id=client_order_id)
            except Exception:
                identity = ""
        if identity and row and str(row.get("position_lifecycle_id", "") or "") != identity:
            try:
                from position_registry import update as registry_update
                if not registry_update(
                    coin,
                    metadata={"position_lifecycle_id": identity},
                    require_persist=True,
                ):
                    return ""
            except Exception as exc:
                logger.error("Position lifecycle identity persistence failed for %s: %s", coin, exc)
                return ""
        return identity

    def _persist_intent(
        self,
        *,
        coin: str,
        intent: dict,
        lifecycle: str,
        mutation_attempted: bool,
    ) -> bool:
        """Durably persist protection intent before any protection POST."""
        try:
            from position_registry import update as registry_update
            row = self._registry_row(coin)
            if not row:
                if self.strict_intent_persistence and callable(
                    getattr(self.client, "list_live_protection_orders", None)
                ):
                    logger.error("Protection intent cannot be persisted for missing registry row %s", coin)
                    return False
                return True
            stored = {
                **dict(intent),
                "lifecycle": lifecycle,
                "mutation_attempted": bool(mutation_attempted),
                "updated_at": self._now_iso(),
            }
            registry_update(
                coin,
                metadata={
                    "protection_intent": stored,
                    "protection_lifecycle": lifecycle,
                    "protection_mutation_attempted": bool(mutation_attempted),
                },
                require_persist=True,
            )
            return True
        except Exception as exc:
            logger.error("Protection intent persistence failed for %s: %s", coin, exc)
            return False

    @staticmethod
    def _intent_identity(
        *,
        position_lifecycle_id: str,
        inst_id: str,
        pos_side: str,
        contracts: float,
        stop_price: float,
        take_profit: float,
        reason: str,
    ) -> str:
        raw = "|".join([
            str(position_lifecycle_id or ""), inst_id.upper(), pos_side.lower(), f"{float(contracts):.12f}",
            f"{float(stop_price):.12f}", f"{float(take_profit):.12f}",
            str(reason or ""),
        ])
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]

    @staticmethod
    def _client_id(identity: str) -> str:
        return canonical_protection_algo_client_id(identity)

    async def _current_position_contracts(self, coin: str, pos_side: str, fallback: float = 0.0) -> float | None:
        """Return authoritative remaining contracts immediately before mutation.

        Production clients must expose get_positions(). Non-strict test doubles
        may use the caller-provided quantity when they predate this boundary.
        None means the read failed or was unavailable; zero is an explicit
        confirmed absence.
        """
        reader = getattr(self.client, "get_positions", None)
        if not callable(reader):
            # The production OKX client always implements this reader.  Keep
            # isolated legacy doubles usable without weakening the production
            # boundary: a real client with a failed reader takes the branch
            # below and fails closed.
            return float(fallback) if fallback > 0 else None
        try:
            rows = await reader()
        except Exception as exc:
            logger.warning("Protection position read failed for %s: %s", coin, exc)
            return None
        target = str(mm.inst_id(coin) or "").upper()
        for row in list(rows or []):
            if str(row.get("instId", "")).upper() != target:
                continue
            row_side = str(row.get("posSide", "")).lower()
            if row_side and row_side != str(pos_side).lower():
                continue
            try:
                return abs(float(row.get("pos", row.get("availPos", 0.0)) or 0.0))
            except (TypeError, ValueError):
                return None
        return 0.0

    @staticmethod
    def _intent_from_row(row: dict) -> dict:
        value = row.get("protection_intent", {})
        return dict(value) if isinstance(value, dict) else {}

    @staticmethod
    def _state_from_row(row: dict) -> str:
        return str(row.get("protection_lifecycle", "") or "").upper()

    def _intent_payload(
        self, *, position_lifecycle_id: str, inst_id: str, pos_side: str, direction: str, contracts: float,
        stop_price: float, take_profit: float, prior_algo_ids: list[str],
        identity: str, reason: str, attempt_count: int = 0,
    ) -> dict:
        return {
            "position_lifecycle_id": str(position_lifecycle_id or ""),
            "protection_transaction_id": identity,
            "instrument": inst_id,
            "pos_side": pos_side,
            "direction": direction,
            "current_contracts": float(contracts),
            "desired_stop_price": float(stop_price),
            "desired_take_profit": float(take_profit),
            "prior_algo_ids": list(prior_algo_ids or []),
            "mutation_identity": identity,
            "algo_client_id": self._client_id(identity),
            "reason": str(reason or "protection_transition"),
            "created_at": self._now_iso(),
            "attempt_count": int(attempt_count),
        }

    def _result_from_rows(
        self, *, rows: list[dict], contracts: float, desired_stop: float,
        metadata, pos_side: str, take_profit: float, reason: str,
        lifecycle: str, superseded: list[str] | None = None,
        cleanup_pending: bool = False, error: str = "", mutation_attempted: bool = False,
    ) -> ProtectionResult:
        first = rows[0] if rows else {}
        confirmed_stop = _normalized_price(
            first.get("slTriggerPx"), metadata,
            position_side=pos_side, purpose="stop",
        ) or 0.0
        sl_id = str(first.get("algoId", "")) if first else ""
        tp_ids = [str(row.get("algoId", "")) for row in rows if row.get("tpTriggerPx")]
        return ProtectionResult(
            protected=bool(rows),
            reason=reason,
            sl_algo_id=sl_id,
            tp_algo_ids=tp_ids,
            protected_contracts=contracts,
            protection_state=PROTECTED if rows else lifecycle,
            confirmed_stop_price=confirmed_stop,
            desired_stop_price=desired_stop,
            superseded_algo_ids=list(superseded or []),
            error=error,
            intent_lifecycle=lifecycle,
            cleanup_pending=cleanup_pending,
            mutation_attempted=mutation_attempted,
        )

    async def _persist_lifecycle(self, coin: str, intent: dict, lifecycle: str, mutation_attempted: bool) -> bool:
        intent = {**dict(intent), "lifecycle": lifecycle, "mutation_attempted": bool(mutation_attempted)}
        return self._persist_intent(
            coin=coin, intent=intent, lifecycle=lifecycle,
            mutation_attempted=mutation_attempted,
        )

    async def _readback_matching(
        self, coin: str, pos_side: str, *, contracts: float,
        stop_price: float, take_profit: float, metadata, force_refresh: bool = False,
    ) -> tuple[dict, list[dict]]:
        view = await self._live_protection_view(coin, pos_side, force_refresh=force_refresh)
        if view["state"] == PROTECTION_STATE_UNKNOWN:
            return view, []
        inst_id = mm.inst_id(coin)
        close_side = _close_side("long" if pos_side == "long" else "short")
        matching = [
            row for row in view.get("orders", [])
            if _protection_matches(
                row, inst_id=inst_id, pos_side=pos_side,
                close_side=close_side, contracts=contracts,
                stop_price=stop_price, take_profit=take_profit,
                metadata=metadata,
            )
        ]
        return view, matching

    async def _recover_uncertain(
        self, *, coin: str, pos_side: str, contracts: float,
        stop_price: float, take_profit: float, metadata, intent: dict,
    ) -> ProtectionResult:
        """Read-only recovery for an intent whose POST outcome is ambiguous."""
        view, matching = await self._readback_matching(
            coin, pos_side, contracts=contracts, stop_price=stop_price,
            take_profit=take_profit, metadata=metadata, force_refresh=True,
        )
        if view["state"] == PROTECTION_STATE_UNKNOWN:
            return ProtectionResult(
                False, REPROTECTION_UNKNOWN, protected_contracts=contracts,
                protection_state=PROTECTION_STATE_UNKNOWN,
                desired_stop_price=stop_price, intent_lifecycle=PROTECTION_SUBMIT_UNCERTAIN,
                recovery_state="STILL_UNKNOWN",
            )
        if matching:
            if intent.get("prior_algo_ids"):
                return await self._retry_cleanup(
                    coin=coin, pos_side=pos_side, contracts=contracts,
                    stop_price=stop_price, take_profit=take_profit,
                    metadata=metadata, intent=intent,
                )
            await self._persist_lifecycle(coin, intent, PROTECTION_CONFIRMED, True)
            return self._result_from_rows(
                rows=matching, contracts=contracts, desired_stop=stop_price,
                metadata=metadata, pos_side=pos_side, take_profit=take_profit,
                reason="protection_recovered_read_only", lifecycle=PROTECTION_CONFIRMED,
            )
        await self._persist_lifecycle(coin, intent, PROTECTION_CONFIRMED_NOT_FOUND, True)
        return ProtectionResult(
            bool(view.get("orders")), PROTECTION_SUBMIT_UNCERTAIN, protected_contracts=contracts,
            protection_state=PROTECTED if view.get("orders") else PROTECTION_STATE_UNKNOWN,
            desired_stop_price=stop_price, intent_lifecycle=PROTECTION_CONFIRMED_NOT_FOUND,
            error="ambiguous_protection_submission_not_found",
            recovery_state="CONFIRMED_NOT_FOUND",
        )

    async def _cancel_cleanup_once(self, *, coin: str, old_rows: list[dict], intent: dict) -> dict:
        """Classify one cleanup request; never replay a durable cleanup attempt."""
        if not old_rows:
            return {"outcome": MUTATION_SUCCESS, "evidence": {"reason": "nothing_to_cleanup"}}
        if bool(intent.get("cleanup_mutation_attempted", False)):
            return {
                "outcome": str(intent.get("cleanup_request_outcome", "TRANSPORT_UNCERTAIN") or "TRANSPORT_UNCERTAIN"),
                "evidence": dict(intent.get("cleanup_response_evidence", {}) or {}),
                "already_attempted": True,
            }

        intent["cleanup_mutation_attempted"] = True
        intent["cleanup_attempted_at"] = self._now_iso()
        if not await self._persist_lifecycle(coin, intent, PROTECTION_CONFIRMED_CLEANUP_PENDING, True):
            return {
                "outcome": "PERSISTENCE_FAILED",
                "evidence": {"reason": "cleanup_attempt_persistence_failed"},
            }
        try:
            response = await self.client.cancel_protective_orders(old_rows)
            classification = classify_mutation_response(
                response,
                expected_items=len(old_rows),
            )
            outcome = classification.outcome
            evidence = classification.to_dict()
        except Exception as exc:
            if str(getattr(exc, "category", "") or "") == "definitive_rejection":
                outcome = MUTATION_DEFINITIVE_REJECTION
                evidence = dict(getattr(exc, "evidence", {}) or {})
                evidence.setdefault("message", str(exc))
            else:
                outcome = "TRANSPORT_UNCERTAIN"
                evidence = {"reason": type(exc).__name__, "message": str(exc)}
        intent["cleanup_request_outcome"] = outcome
        intent["cleanup_response_evidence"] = evidence
        await self._persist_lifecycle(coin, intent, PROTECTION_CONFIRMED_CLEANUP_PENDING, True)
        return {"outcome": outcome, "evidence": evidence}

    async def _retry_cleanup(
        self, *, coin: str, pos_side: str, contracts: float, stop_price: float,
        take_profit: float, metadata, intent: dict,
    ) -> ProtectionResult:
        """Retry only old-order cleanup; this path is forbidden to POST."""
        view, matching = await self._readback_matching(
            coin, pos_side, contracts=contracts, stop_price=stop_price,
            take_profit=take_profit, metadata=metadata,
        )
        if view["state"] == PROTECTION_STATE_UNKNOWN or not matching:
            return ProtectionResult(
                bool(matching), PROTECTION_CONFIRMED_CLEANUP_PENDING,
                protected_contracts=contracts,
                protection_state=PROTECTED if matching else PROTECTION_STATE_UNKNOWN,
                desired_stop_price=stop_price,
                intent_lifecycle=PROTECTION_CONFIRMED_CLEANUP_PENDING,
                cleanup_pending=True, error="confirmed_replacement_not_visible",
            )
        confirmed_ids = {str(row.get("algoId", "")) for row in matching}
        old_ids = set(str(value) for value in intent.get("prior_algo_ids", []) or []) - confirmed_ids
        old_rows = [row for row in view.get("orders", []) if str(row.get("algoId", "")) in old_ids]
        cleanup_result = await self._cancel_cleanup_once(
            coin=coin, old_rows=old_rows, intent=intent,
        )
        final_view, final_matching = await self._readback_matching(
            coin, pos_side, contracts=contracts, stop_price=stop_price,
            take_profit=take_profit, metadata=metadata, force_refresh=True,
        )
        final_position = await self._current_position_contracts(coin, pos_side, fallback=contracts)
        remaining_old = [
            row for row in final_view.get("orders", [])
            if str(row.get("algoId", "")) in old_ids
        ] if final_view.get("state") != PROTECTION_STATE_UNKNOWN else []
        if final_position is None or final_position <= 0 or final_view["state"] == PROTECTION_STATE_UNKNOWN or not final_matching:
            await self._persist_lifecycle(coin, intent, PROTECTION_SUBMIT_UNCERTAIN, True)
            return ProtectionResult(
                bool(final_matching), PROTECTION_STATE_UNKNOWN,
                protected_contracts=contracts, protection_state=PROTECTION_STATE_UNKNOWN,
                desired_stop_price=stop_price, intent_lifecycle=PROTECTION_STATE_UNKNOWN,
                error="final_protection_readback_failed",
            )
        if remaining_old:
            await self._persist_lifecycle(coin, intent, PROTECTION_CONFIRMED_CLEANUP_PENDING, True)
            return self._result_from_rows(
                rows=final_matching, contracts=contracts, desired_stop=stop_price,
                metadata=metadata, pos_side=pos_side, take_profit=take_profit,
                reason=PROTECTION_CONFIRMED_CLEANUP_PENDING,
                lifecycle=PROTECTION_CONFIRMED_CLEANUP_PENDING,
                superseded=list(old_ids), cleanup_pending=True,
                error=f"cleanup_{cleanup_result.get('outcome', 'unknown').lower()}_old_protection_remains",
            )
        await self._persist_lifecycle(coin, intent, PROTECTION_CONFIRMED, True)
        return self._result_from_rows(
            rows=final_matching, contracts=contracts, desired_stop=stop_price,
            metadata=metadata, pos_side=pos_side, take_profit=take_profit,
            reason=PROTECTION_CONFIRMED, lifecycle=PROTECTION_CONFIRMED,
        )

    async def _transition_protection(
        self, *, coin: str, direction: str, contracts: float, stop_price: float,
        take_profit: float, client_order_id: str, paper: bool, reason: str,
        monotonic: bool, allow_mutation: bool, emergency_on_missing: bool,
        tp_owner: str = "", position_lifecycle_id: str = "",
    ) -> ProtectionResult:
        normalized_owner = normalize_persisted_owner(tp_owner)
        if normalized_owner not in VALID_TP_OWNERS:
            if float(take_profit or 0.0) > 0.0:
                logger.warning(
                    "Suppressing unowned exchange TP for %s owner=%s",
                    coin,
                    normalized_owner or "EMPTY",
                )
            take_profit = 0.0
        else:
            take_profit = effective_exchange_take_profit(normalized_owner, take_profit)
        if contracts <= 0 or stop_price <= 0:
            return ProtectionResult(False, "invalid_protection_inputs")
        local_row = self._registry_row(coin)
        if local_row:
            try:
                from position_registry import (
                    exposure_status,
                    EXPOSURE_UNKNOWN,
                    EXPOSURE_TERMINAL,
                    EXCHANGE_CLOSED_AWAITING_RECONCILIATION,
                )
                # Loaded registry rows always carry the normalized field. A
                # fieldless protection-intent record is not local exposure
                # evidence, while a legacy position row remains conservative.
                local_status = "" if "exposure_truth_status" not in local_row else exposure_status(local_row)
                if local_status in {
                    EXPOSURE_UNKNOWN,
                    EXPOSURE_TERMINAL,
                    EXCHANGE_CLOSED_AWAITING_RECONCILIATION,
                }:
                    return ProtectionResult(
                        False,
                        "local_exposure_not_mutation_actionable",
                        protected_contracts=contracts,
                        protection_state=PROTECTION_STATE_UNKNOWN,
                        desired_stop_price=stop_price,
                        error=local_status,
                    )
            except Exception:
                return ProtectionResult(
                    False,
                    "local_exposure_classification_unavailable",
                    protection_state=PROTECTION_STATE_UNKNOWN,
                )
        if paper:
            return ProtectionResult(True, "paper_local_protection", protected_contracts=contracts, protection_state=PROTECTED)
        pos_side = "long" if direction.lower() in {"long", "buy"} else "short"
        close_side = _close_side(direction)
        inst_id = mm.inst_id(coin)
        metadata = _metadata_for(self.client, coin, self.instruments)
        canonical_discovery = callable(getattr(self.client, "list_live_protection_orders", None))
        if metadata is None and canonical_discovery:
            return ProtectionResult(False, "protection_state_unknown", protection_state=PROTECTION_STATE_UNKNOWN)
        async with self._reprotect_lock(coin, pos_side):
            row = self._registry_row(coin)
            prior_lifecycle = self._state_from_row(row)
            prior_intent = self._intent_from_row(row)
            lifecycle_id = self._position_lifecycle_id(
                coin,
                row=row,
                prior_intent=prior_intent,
                client_order_id=client_order_id,
                provided=position_lifecycle_id,
            )
            if prior_lifecycle == PROTECTION_CONFIRMED_CLEANUP_PENDING:
                return await self._retry_cleanup(
                    coin=coin, pos_side=pos_side, contracts=contracts,
                    stop_price=stop_price, take_profit=take_profit,
                    metadata=metadata, intent=prior_intent,
                )
            identity = self._intent_identity(
                position_lifecycle_id=lifecycle_id,
                inst_id=inst_id, pos_side=pos_side, contracts=contracts,
                stop_price=stop_price, take_profit=take_profit, reason=reason,
            )
            if prior_lifecycle in {PROTECTION_SUBMISSION_ATTEMPTED, PROTECTION_SUBMIT_UNCERTAIN}:
                if str(prior_intent.get("mutation_identity", "")) == identity:
                    return await self._recover_uncertain(
                        coin=coin, pos_side=pos_side, contracts=contracts,
                        stop_price=stop_price, take_profit=take_profit,
                        metadata=metadata, intent=prior_intent,
                    )
                return ProtectionResult(
                    False, PROTECTION_SUBMIT_UNCERTAIN, protected_contracts=contracts,
                    protection_state=PROTECTION_STATE_UNKNOWN,
                    desired_stop_price=stop_price, intent_lifecycle=prior_lifecycle,
                    error="prior_protection_intent_unresolved",
                )
            if (
                prior_lifecycle == REPROTECTION_FAILED
                and str(prior_intent.get("mutation_identity", "")) == identity
            ):
                confirmed_stop = float(row.get("exchange_confirmed_stop_price", 0.0) or 0.0)
                protected = str(row.get("exchange_protection_status", "")).upper() in {
                    "STOP_CONFIRMED", "EXCHANGE_PROTECTED", "PROTECTED",
                } and confirmed_stop > 0
                return ProtectionResult(
                    protected, REPROTECTION_FAILED, protected_contracts=contracts,
                    protection_state=PROTECTED if protected else PROTECTION_STATE_UNKNOWN,
                    confirmed_stop_price=confirmed_stop,
                    desired_stop_price=stop_price,
                    intent_lifecycle=REPROTECTION_FAILED,
                    error="protection_intent_already_attempted",
                )
            if (
                prior_lifecycle == PROTECTION_CONFIRMED_NOT_FOUND
                and str(prior_intent.get("mutation_identity", "")) == identity
            ):
                identity = self._intent_identity(
                    position_lifecycle_id=lifecycle_id,
                    inst_id=inst_id, pos_side=pos_side, contracts=contracts,
                    stop_price=stop_price, take_profit=take_profit,
                    reason=f"{reason}:controlled-retry:{prior_intent.get('created_at', '')}",
                )
            view = await self._confirmed_protection_view(
                coin, pos_side, expected_attached=bool(client_order_id),
            )
            if view["state"] == PROTECTION_STATE_UNKNOWN:
                return ProtectionResult(False, "protection_state_unknown", protection_state=PROTECTION_STATE_UNKNOWN)
            scoped = [
                row for row in view.get("orders", [])
                if str(row.get("state", "live")).lower() in _LIVE_PROTECTION_STATES
                and str(row.get("instId", "")).upper() == inst_id.upper()
                and (row.get("_legacy_protection_view") or str(row.get("posSide", "")).lower() == pos_side)
            ]
            if normalized_owner not in VALID_TP_OWNERS and any(self._tp_value(row) > 0 for row in scoped):
                # Ownership is unresolved: preserve already-attached profit
                # protection and wait for explicit evidence instead of
                # cancelling it or creating a competing replacement.
                ids = [str(item.get("algoId", "")) for item in scoped if item.get("algoId")]
                return ProtectionResult(
                    True,
                    "tp_owner_unresolved_preserved",
                    ids[0] if ids else "",
                    ids,
                    contracts,
                    protection_state=PROTECTED,
                    desired_stop_price=stop_price,
                    intent_lifecycle=PROTECTION_CONFIRMED,
                    error="tp_owner_unresolved",
                )
            matching = [
                row for row in scoped
                if _protection_matches(
                    row, inst_id=inst_id, pos_side=pos_side, close_side=close_side,
                    contracts=contracts, stop_price=stop_price, take_profit=take_profit,
                    metadata=metadata,
                )
            ]
            if matching:
                confirmed_stop = _normalized_price(
                    matching[0].get("slTriggerPx"), metadata,
                    position_side=pos_side, purpose="stop",
                ) or float(stop_price)
                ids = [str(item.get("algoId", "")) for item in matching if item.get("algoId")]
                if monotonic:
                    return ProtectionResult(
                        True, REPROTECTION_UNCHANGED,
                        ids[0] if ids else "", ids[1:], contracts,
                        protection_state=PROTECTED,
                        confirmed_stop_price=confirmed_stop,
                        desired_stop_price=confirmed_stop,
                        intent_lifecycle=PROTECTION_CONFIRMED,
                    )
                return ProtectionResult(
                    True,
                    "existing_exchange_protection_duplicate" if len(matching) > 1 else "existing_exchange_protection",
                    ids[0] if ids else "", ids, contracts, protection_state=PROTECTED,
                    confirmed_stop_price=confirmed_stop, desired_stop_price=confirmed_stop,
                    intent_lifecycle=PROTECTION_CONFIRMED,
                )
            if monotonic:
                if not scoped:
                    return ProtectionResult(False, REPROTECTION_UNKNOWN, protection_state=PROTECTION_STATE_UNKNOWN)
                old_stops = [self._stop_value(item) for item in scoped if self._stop_value(item) > 0]
                current_stop = max(old_stops) if pos_side == "long" else min(old_stops)
                current_stop = _normalized_price(current_stop, metadata, position_side=pos_side, purpose="stop") or current_stop
                desired_norm = _normalized_price(stop_price, metadata, position_side=pos_side, purpose="stop") or float(stop_price)
                if (pos_side == "long" and desired_norm <= current_stop) or (pos_side == "short" and desired_norm >= current_stop):
                    return ProtectionResult(True, REPROTECTION_UNCHANGED, scoped[0].get("algoId", ""), [], contracts,
                                            protection_state=PROTECTED, confirmed_stop_price=current_stop,
                                            desired_stop_price=desired_norm, intent_lifecycle=PROTECTION_CONFIRMED)
                preserved_tp = max((self._tp_value(item) for item in scoped), default=0.0)
                if normalized_owner not in {LOCAL_STAGED, NO_PROFIT_TP} and preserved_tp > 0:
                    take_profit = preserved_tp
                stop_price = desired_norm
                identity = self._intent_identity(
                    position_lifecycle_id=lifecycle_id,
                    inst_id=inst_id, pos_side=pos_side, contracts=contracts,
                    stop_price=stop_price, take_profit=take_profit, reason=reason,
                ) if lifecycle_id else ""
            if not allow_mutation:
                return ProtectionResult(False, REPROTECTION_PENDING, protected_contracts=contracts,
                                        protection_state=PROTECTED if scoped else PROTECTION_STATE_UNKNOWN,
                                        desired_stop_price=stop_price, intent_lifecycle=PROTECTION_INTENT_RESERVED,
                                         error="mutation_not_allowed")
            if not lifecycle_id:
                current_stop = 0.0
                if scoped:
                    current_stop = max(
                        (self._stop_value(item) for item in scoped if self._stop_value(item) > 0),
                        default=0.0,
                    ) if pos_side == "long" else min(
                        (self._stop_value(item) for item in scoped if self._stop_value(item) > 0),
                        default=0.0,
                    )
                return ProtectionResult(
                    bool(scoped), REPROTECTION_FAILED,
                    protected_contracts=contracts,
                    protection_state=PROTECTED if scoped else PROTECTION_STATE_UNKNOWN,
                    confirmed_stop_price=current_stop,
                    desired_stop_price=stop_price,
                    intent_lifecycle=REPROTECTION_FAILED,
                    error="position_lifecycle_identity_unavailable",
                )
            initial_contracts = await self._current_position_contracts(coin, pos_side, fallback=contracts)
            if initial_contracts is None or initial_contracts <= 0:
                return ProtectionResult(
                    False, REPROTECTION_UNKNOWN, protected_contracts=contracts,
                    protection_state=PROTECTED if scoped else PROTECTION_STATE_UNKNOWN,
                    desired_stop_price=stop_price,
                    intent_lifecycle=PROTECTION_INTENT_RESERVED,
                    error="current_position_unavailable_or_closed",
                )
            contracts = initial_contracts
            identity = self._intent_identity(
                position_lifecycle_id=lifecycle_id,
                inst_id=inst_id, pos_side=pos_side, contracts=contracts,
                stop_price=stop_price, take_profit=take_profit, reason=reason,
            )
            prior_ids = [str(item.get("algoId", "")) for item in scoped if item.get("algoId")]
            intent = self._intent_payload(
                position_lifecycle_id=lifecycle_id,
                inst_id=inst_id, pos_side=pos_side, direction=direction,
                contracts=contracts, stop_price=stop_price, take_profit=take_profit,
                prior_algo_ids=prior_ids, identity=identity, reason=reason,
            )
            if not await self._persist_lifecycle(coin, intent, PROTECTION_INTENT_RESERVED, False):
                return ProtectionResult(False, REPROTECTION_FAILED, protected_contracts=contracts,
                                        protection_state=PROTECTED if scoped else PROTECTION_STATE_UNKNOWN,
                                        desired_stop_price=stop_price, intent_lifecycle=PROTECTION_INTENT_RESERVED,
                                        error="protection_intent_persistence_failed")
            algo_client_id = self._client_id(identity)
            try:
                validate_client_id(algo_client_id, field="algoClOrdId")
            except InvalidOKXClientID as exc:
                await self._persist_lifecycle(coin, intent, REPROTECTION_FAILED, False)
                return ProtectionResult(
                    bool(scoped), REPROTECTION_FAILED, protected_contracts=contracts,
                    protection_state=PROTECTED if scoped else PROTECTION_STATE_UNKNOWN,
                    confirmed_stop_price=(
                        _normalized_price(scoped[0].get("slTriggerPx"), metadata, position_side=pos_side, purpose="stop")
                        if scoped else 0.0
                    ) or 0.0,
                    desired_stop_price=stop_price,
                    intent_lifecycle=REPROTECTION_FAILED,
                    error=f"invalid_local_client_id:{exc}",
                )
            intent["attempt_count"] = 1
            if not await self._persist_lifecycle(coin, intent, PROTECTION_SUBMISSION_ATTEMPTED, True):
                return ProtectionResult(False, REPROTECTION_FAILED, protected_contracts=contracts,
                                        protection_state=PROTECTED if scoped else PROTECTION_STATE_UNKNOWN,
                                        desired_stop_price=stop_price, intent_lifecycle=PROTECTION_SUBMISSION_ATTEMPTED,
                                        error="protection_attempt_persistence_failed")
            fresh_contracts = await self._current_position_contracts(coin, pos_side, fallback=contracts)
            if fresh_contracts is None or fresh_contracts <= 0:
                await self._persist_lifecycle(coin, intent, PROTECTION_INTENT_RESERVED, False)
                return ProtectionResult(False, REPROTECTION_UNKNOWN, protected_contracts=contracts,
                                        protection_state=PROTECTED if scoped else PROTECTION_STATE_UNKNOWN,
                                        desired_stop_price=stop_price, intent_lifecycle=PROTECTION_INTENT_RESERVED,
                                        error="current_position_unavailable_or_closed")
            contracts = fresh_contracts
            try:
                response = await self.client.place_protective_order(
                    coin=coin, direction=direction, contracts=contracts,
                    stop_price=stop_price, take_profit=take_profit,
                    client_order_id=algo_client_id,
                )
            except Exception as exc:
                if getattr(exc, "category", "") == "definitive_rejection":
                    intent["rejection_evidence"] = dict(
                        getattr(exc, "evidence", {}) or {
                            "top_level_code": str(getattr(exc, "code", "") or ""),
                            "item_s_code": str(getattr(exc, "s_code", "") or ""),
                            "message": str(exc),
                            "timestamp": self._now_iso(),
                        }
                    )
                    await self._persist_lifecycle(coin, intent, REPROTECTION_FAILED, True)
                    return self._result_from_rows(
                        rows=scoped, contracts=contracts, desired_stop=stop_price,
                        metadata=metadata, pos_side=pos_side, take_profit=take_profit,
                        reason=REPROTECTION_FAILED, lifecycle=REPROTECTION_FAILED,
                        error=f"{getattr(exc, 's_code', '') or getattr(exc, 'code', '')}:{exc}",
                        mutation_attempted=True,
                    )
                await self._persist_lifecycle(coin, intent, PROTECTION_SUBMIT_UNCERTAIN, True)
                return ProtectionResult(False, REPROTECTION_PENDING if monotonic else PROTECTION_SUBMIT_UNCERTAIN, protected_contracts=contracts,
                                        protection_state=PROTECTED if scoped else PROTECTION_STATE_UNKNOWN,
                                        desired_stop_price=stop_price, intent_lifecycle=PROTECTION_SUBMIT_UNCERTAIN,
                    error=type(exc).__name__, mutation_attempted=True)
            classification = classify_mutation_response(
                response,
                expected_items=1,
                submitted_client_id=algo_client_id,
            )
            if classification.outcome == MUTATION_DEFINITIVE_REJECTION:
                intent["rejection_evidence"] = classification.to_dict()
                await self._persist_lifecycle(coin, intent, REPROTECTION_FAILED, True)
                return self._result_from_rows(
                    rows=scoped, contracts=contracts, desired_stop=stop_price,
                    metadata=metadata, pos_side=pos_side, take_profit=take_profit,
                    reason=REPROTECTION_FAILED, lifecycle=REPROTECTION_FAILED,
                    error=f"{classification.item_s_code or classification.top_level_code}:{classification.item_s_msg or classification.top_level_msg or 'protection_rejected'}",
                    mutation_attempted=True,
                )
            if classification.outcome == MUTATION_READBACK_REQUIRED:
                await self._persist_lifecycle(coin, intent, PROTECTION_SUBMIT_UNCERTAIN, True)
                return ProtectionResult(
                    False, REPROTECTION_PENDING if monotonic else PROTECTION_SUBMIT_UNCERTAIN,
                    protected_contracts=contracts,
                    protection_state=PROTECTION_STATE_UNKNOWN,
                    desired_stop_price=stop_price,
                    intent_lifecycle=PROTECTION_SUBMIT_UNCERTAIN,
                    error="mutation_response_readback_required",
                    mutation_attempted=True,
                )
            if not canonical_discovery:
                response_data = (response or {}).get("data", []) or []
                algo_id = str(response_data[0].get("algoId", "")) if response_data else ""
                new_matching = [{
                    "algoId": algo_id,
                    "instId": inst_id,
                    "ordType": "oco" if take_profit else "conditional",
                    "posSide": pos_side,
                    "side": close_side,
                    "sz": str(contracts),
                    "reduceOnly": "true",
                    "state": "live",
                    "slTriggerPx": str(stop_price),
                    "tpTriggerPx": str(take_profit or 0.0),
                }]
                readback = {"state": "READ_COMPLETE", "orders": list(new_matching)}
            else:
                readback, new_matching = await self._readback_matching(
                    coin, pos_side, contracts=contracts, stop_price=stop_price,
                    take_profit=take_profit, metadata=metadata, force_refresh=True,
                )
            if readback["state"] == PROTECTION_STATE_UNKNOWN or not new_matching:
                await self._persist_lifecycle(coin, intent, PROTECTION_SUBMIT_UNCERTAIN, True)
                return ProtectionResult(False, REPROTECTION_PENDING if monotonic else PROTECTION_SUBMIT_UNCERTAIN, protected_contracts=contracts,
                                        protection_state=PROTECTION_STATE_UNKNOWN if readback["state"] == PROTECTION_STATE_UNKNOWN else PROTECTED,
                                        desired_stop_price=stop_price, intent_lifecycle=PROTECTION_SUBMIT_UNCERTAIN,
                                        error="replacement_readback_missing")
            new_ids = {str(item.get("algoId", "")) for item in new_matching}
            superseded = [item for item in scoped if str(item.get("algoId", "")) not in new_ids]
            superseded_ids = [str(item.get("algoId", "")) for item in superseded if item.get("algoId")]
            cleanup_result = {"outcome": MUTATION_SUCCESS, "evidence": {"reason": "no_superseded_protection"}}
            if superseded:
                intent["prior_algo_ids"] = superseded_ids
                cleanup_result = await self._cancel_cleanup_once(
                    coin=coin, old_rows=superseded, intent=intent,
                )
            final_position = await self._current_position_contracts(coin, pos_side, fallback=contracts)
            if not canonical_discovery:
                final_view, final_matching = {"state": "READ_COMPLETE"}, list(new_matching)
            else:
                final_view, final_matching = await self._readback_matching(
                    coin, pos_side, contracts=contracts, stop_price=stop_price,
                    take_profit=take_profit, metadata=metadata, force_refresh=True,
                )
            if final_position is None or final_position <= 0 or final_view["state"] == PROTECTION_STATE_UNKNOWN or not final_matching:
                await self._persist_lifecycle(coin, intent, PROTECTION_SUBMIT_UNCERTAIN, True)
                return ProtectionResult(bool(final_matching), PROTECTION_STATE_UNKNOWN,
                                        protected_contracts=contracts,
                                        protection_state=PROTECTION_STATE_UNKNOWN,
                                        desired_stop_price=stop_price,
                                        intent_lifecycle=PROTECTION_SUBMIT_UNCERTAIN,
                                        error="final_protection_readback_failed")
            remaining_old = [
                row for row in final_view.get("orders", [])
                if str(row.get("algoId", "")) in set(superseded_ids)
            ] if final_view.get("state") != PROTECTION_STATE_UNKNOWN else []
            if remaining_old:
                await self._persist_lifecycle(coin, intent, PROTECTION_CONFIRMED_CLEANUP_PENDING, True)
                return self._result_from_rows(
                    rows=final_matching, contracts=contracts, desired_stop=stop_price,
                    metadata=metadata, pos_side=pos_side, take_profit=take_profit,
                    reason=PROTECTION_CONFIRMED_CLEANUP_PENDING,
                    lifecycle=PROTECTION_CONFIRMED_CLEANUP_PENDING,
                    superseded=superseded_ids, cleanup_pending=True,
                    error=f"cleanup_{cleanup_result.get('outcome', 'unknown').lower()}_old_protection_remains",
                    mutation_attempted=True,
                )
            await self._persist_lifecycle(coin, intent, PROTECTION_CONFIRMED, True)
            return self._result_from_rows(
                rows=final_matching, contracts=contracts, desired_stop=stop_price,
                metadata=metadata, pos_side=pos_side, take_profit=take_profit,
                reason=REPROTECTION_CONFIRMED if monotonic else "exchange_protection_created",
                lifecycle=PROTECTION_CONFIRMED, superseded=superseded_ids,
                mutation_attempted=True,
            )

    async def ensure(
        self, *, coin: str, direction: str, contracts: float, stop_price: float,
        take_profit: float = 0.0, client_order_id: str = "", paper: bool = False,
        tp_owner: str = "", position_lifecycle_id: str = "",
    ) -> ProtectionResult:
        result = await self._transition_protection(
            coin=coin, direction=direction, contracts=contracts, stop_price=stop_price,
            take_profit=take_profit, client_order_id=client_order_id, paper=paper,
            reason="entry_protection", monotonic=False, allow_mutation=True,
            emergency_on_missing=True,
            tp_owner=tp_owner,
            position_lifecycle_id=position_lifecycle_id,
        )
        if (
            not result.protected
            and result.reason in {REPROTECTION_FAILED, REPROTECTION_PENDING, PROTECTION_SUBMIT_UNCERTAIN}
            and result.error != "protection_intent_persistence_failed"
            and result.protection_state != PROTECTED
        ):
            try:
                if not callable(self._emergency_close):
                    raise RuntimeError("durable_emergency_close_router_unavailable")
                close_result = await self._emergency_close(
                    coin=coin,
                    pos_side="long" if direction.lower() in {"long", "buy"} else "short",
                    contracts=contracts,
                    source="protection_unprotected_fill",
                    client_order_id=client_order_id,
                )
                close_status = str(dict(close_result or {}).get("status", "") or "")
                if close_status in {"CLOSE_FILLED", "CLOSE_ALREADY_ZERO"}:
                    result.emergency_closed = True
                    result.reason = "emergency_close_unprotected_fill"
                else:
                    result.reason = f"unprotected_emergency_close_pending:{close_status or 'unknown'}"
            except Exception as exc:
                logger.critical("Emergency close failed for unprotected %s: %s", coin, exc)
                result.reason = f"unprotected_emergency_close_failed:{type(exc).__name__}"
        return result

    async def reprotect(
        self, *, coin: str, direction: str, current_contracts: float,
        desired_stop_price: float, take_profit: float = 0.0,
        client_order_id: str = "", paper: bool = False,
        allow_mutation: bool = True,
        tp_owner: str = "", position_lifecycle_id: str = "",
    ) -> ProtectionResult:
        result = await self._transition_protection(
            coin=coin, direction=direction, contracts=current_contracts,
            stop_price=desired_stop_price, take_profit=take_profit,
            client_order_id=client_order_id, paper=paper,
            reason="dynamic_reprotection", monotonic=True,
            allow_mutation=allow_mutation, emergency_on_missing=False,
            tp_owner=tp_owner,
            position_lifecycle_id=position_lifecycle_id,
        )
        if result.reason == "protection_state_unknown":
            result.reason = REPROTECTION_UNKNOWN
        elif result.reason == "paper_local_protection":
            result.reason = REPROTECTION_CONFIRMED
        return result

    async def _live_protection_view(self, coin: str, pos_side: str, *, force_refresh: bool = False) -> dict:
        """Read all relevant protection feeds without mixing entry orders."""
        discover = getattr(self.client, "list_live_protection_orders", None)
        if discover is not None:
            try:
                try:
                    result = await discover(coin, pos_side=pos_side, force_refresh=force_refresh, priority="P0")
                except TypeError:
                    result = await discover(coin, pos_side=pos_side)
            except Exception as exc:
                return {
                    "state": PROTECTION_STATE_UNKNOWN,
                    "orders": [],
                    "errors": [type(exc).__name__],
                }
            if not isinstance(result, dict) or not bool(result.get("ok", False)):
                errors = (result or {}).get("errors", []) if isinstance(result, dict) else []
                return {
                    "state": PROTECTION_STATE_UNKNOWN,
                    "orders": [],
                    "errors": errors,
                }
            return {
                "state": CONFIRMED_MISSING if not result.get("orders") else "READ_COMPLETE",
                "orders": list(result.get("orders", []) or []),
                "errors": [],
            }
        # Compatibility for isolated test doubles and older clients.  The
        # production OKXClient always uses the canonical merged view above.
        try:
            try:
                rows = list(await self.client.get_algo_orders(coin, force_refresh=force_refresh, priority="P0") or [])
            except TypeError:
                rows = list(await self.client.get_algo_orders(coin) or [])
        except Exception as exc:
            return {"state": PROTECTION_STATE_UNKNOWN, "orders": [], "errors": [type(exc).__name__]}
        return {
            "state": CONFIRMED_MISSING if not rows else "READ_COMPLETE",
            "orders": [{**row, "_legacy_protection_view": True} for row in rows],
            "errors": [],
        }

    async def _confirmed_protection_view(self, coin: str, pos_side: str, *, expected_attached: bool) -> dict:
        first = await self._live_protection_view(coin, pos_side)
        if first["state"] == PROTECTION_STATE_UNKNOWN or first["orders"] or not expected_attached:
            return first

        # An entry-attached OCO can be briefly absent immediately after the
        # fill. Recheck once, with a short bounded delay, before declaring the
        # exchange definitively unprotected.
        await asyncio.sleep(_CONFIRMATION_DELAY_SEC)
        second = await self._live_protection_view(coin, pos_side, force_refresh=True)
        if second["state"] == PROTECTION_STATE_UNKNOWN:
            return second
        return second if second["orders"] else {**second, "state": CONFIRMED_MISSING}

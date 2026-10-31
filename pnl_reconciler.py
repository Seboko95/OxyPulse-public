"""OKX fill, fee, funding, and realized-PnL reconciliation."""
from __future__ import annotations

import json
import asyncio
import time
from pathlib import Path
from threading import RLock
from typing import Any

STATUSES = {"PROVISIONAL", "AWAITING_EXCHANGE_DATA", "RECONCILED", "ROUNDING_DIFFERENCE", "MISMATCH", "UNRESOLVED"}
FINAL_STATUSES = {"RECONCILED", "ROUNDING_DIFFERENCE"}
EVIDENCE_STATUSES = {"SUCCESS_DATA", "SUCCESS_EMPTY", "QUERY_FAILED", "UNKNOWN"}


def _f(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _epoch_seconds(value: Any) -> float:
    numeric = _f(value, 0.0)
    if numeric > 1_000_000_000_000:
        return numeric / 1000.0
    if numeric > 1_000_000_000:
        return numeric
    if isinstance(value, str) and value:
        try:
            from datetime import datetime
            return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
        except Exception:
            return 0.0
    return 0.0


def _first_number(row: dict, *keys: str) -> float:
    for key in keys:
        if key in row and row.get(key) not in {None, ""}:
            return _f(row.get(key))
    return 0.0


def _fill_qty(row: dict) -> float:
    return abs(_first_number(row, "fillSz", "fill_size", "accFillSz", "sz", "qty", "quantity"))


def _fill_px(row: dict) -> float:
    return _first_number(row, "fillPx", "fill_price", "avgPx", "price")


def _weighted_average(rows: list[dict], value_keys: tuple[str, ...]) -> float:
    weighted = 0.0
    quantity = 0.0
    for row in rows:
        qty = _fill_qty(row)
        value = _first_number(row, *value_keys)
        if qty > 0 and value > 0:
            weighted += qty * value
            quantity += qty
    return weighted / quantity if quantity > 0 else 0.0


def _dedupe_rows(rows: list[dict], keys: tuple[str, ...]) -> list[dict]:
    seen: set[str] = set()
    result: list[dict] = []
    for row in rows:
        identity = next(
            (str(row.get(key)) for key in keys if row.get(key) not in {None, ""}),
            json.dumps(row, sort_keys=True),
        )
        if identity in seen:
            continue
        seen.add(identity)
        result.append(dict(row))
    return result


def _evidence_record(
    *,
    status: str = "UNKNOWN",
    data: list[dict] | None = None,
    source: str = "",
    reason: str = "",
    timestamp: float | None = None,
    **extra: Any,
) -> dict:
    normalized = str(status or "UNKNOWN").upper()
    if normalized not in EVIDENCE_STATUSES:
        normalized = "UNKNOWN"
    rows = list(data or [])
    if normalized == "SUCCESS_DATA" and not rows:
        normalized = "SUCCESS_EMPTY"
    return {
        "status": normalized,
        "ok": normalized in {"SUCCESS_DATA", "SUCCESS_EMPTY"},
        "data": rows,
        "source": str(source or ""),
        "reason": str(reason or ""),
        "timestamp": float(timestamp or time.time()),
        **extra,
    }


def _normalize_evidence(value: Any, *, data_key: str = "data", source: str = "") -> dict:
    """Normalize a structured source result without treating missing provenance as success."""
    if not isinstance(value, dict):
        return _evidence_record(source=source, reason="evidence_missing")
    raw_data = value.get(data_key, value.get("data", []))
    rows = list(raw_data or []) if isinstance(raw_data, (list, tuple)) else []
    status = str(value.get("status", "") or "").upper()
    if status not in EVIDENCE_STATUSES:
        ok = value.get("ok")
        if ok is True:
            status = "SUCCESS_DATA" if rows else "SUCCESS_EMPTY"
        elif ok is False:
            status = "QUERY_FAILED"
        else:
            status = "UNKNOWN"
    return _evidence_record(
        status=status,
        data=rows,
        source=str(value.get("source", source) or source),
        reason=str(value.get("reason", value.get("error", "")) or ""),
        timestamp=_f(value.get("timestamp"), time.time()),
        **{
            key: value[key]
            for key in ("realized_pnl_present", "fee_data_complete")
            if key in value
        },
    )


async def _query_source_evidence(
    client: Any,
    *,
    status_method: str,
    legacy_method: str,
    data_key: str,
    source: str,
    **kwargs: Any,
) -> dict:
    """Use explicit client status APIs; legacy empty-only APIs remain UNKNOWN."""
    status_reader = getattr(client, status_method, None)
    if callable(status_reader):
        try:
            return _normalize_evidence(await status_reader(**kwargs), data_key=data_key, source=source)
        except Exception as exc:
            return _evidence_record(source=source, status="QUERY_FAILED", reason=f"{type(exc).__name__}:{exc}")
    reader = getattr(client, legacy_method, None)
    if not callable(reader):
        return _evidence_record(source=source, status="QUERY_FAILED", reason="source_method_missing")
    try:
        rows = await reader(**kwargs)
    except Exception as exc:
        return _evidence_record(source=source, status="QUERY_FAILED", reason=f"{type(exc).__name__}:{exc}")
    rows = list(rows or [])
    if rows:
        return _evidence_record(status="SUCCESS_DATA", data=rows, source=source, reason="legacy_nonempty_response")
    return _evidence_record(source=source, status="UNKNOWN", reason="legacy_empty_response_without_provenance")


def _can_finalize_exchange_pnl(
    *,
    entry_fills: list[dict],
    exit_fills: list[dict],
    fee_data_complete: bool,
    evidence: dict[str, dict],
    exchange_realized_pnl: float | None,
    entry_contracts: float,
    exit_contracts: float,
) -> tuple[bool, list[str]]:
    """Single finalization gate for all authoritative exchange economics."""
    reasons: list[str] = []
    fills = evidence.get("fills", _evidence_record(source="okx.fills"))
    funding = evidence.get("funding", _evidence_record(source="okx.bills"))
    history = evidence.get("position_history", _evidence_record(source="okx.position_history"))
    fees = evidence.get("fees", _evidence_record(source="okx.fill_fees"))
    if fills.get("status") != "SUCCESS_DATA":
        reasons.append(f"fills_{fills.get('status', 'UNKNOWN').lower()}")
    if not entry_fills:
        reasons.append("entry_fills_missing")
    if not exit_fills:
        reasons.append("exit_fills_missing")
    if funding.get("status") not in {"SUCCESS_DATA", "SUCCESS_EMPTY"}:
        reasons.append(f"funding_{funding.get('status', 'UNKNOWN').lower()}")
    if history.get("status") != "SUCCESS_DATA":
        reasons.append(f"position_history_{history.get('status', 'UNKNOWN').lower()}")
    if not bool(history.get("realized_pnl_present", False)):
        reasons.append("realized_pnl_evidence_missing")
    if fees.get("status") != "SUCCESS_DATA":
        reasons.append(f"fees_{fees.get('status', 'UNKNOWN').lower()}")
    if not fee_data_complete:
        reasons.append("fee_fields_incomplete")
    if exchange_realized_pnl is None:
        reasons.append("exchange_realized_pnl_missing")
    if entry_contracts <= 0:
        reasons.append("entry_quantity_missing")
    if exit_contracts <= 0:
        reasons.append("exit_quantity_missing")
    if entry_contracts > 0 and exit_contracts > entry_contracts + 1e-9:
        reasons.append("exit_quantity_exceeds_entry")
    return not reasons, reasons


def _compute_gross_price_pnl(
    entry_fills: list[dict],
    exit_fills: list[dict],
    *,
    direction: str,
    entry_notional_usdt: float = 0.0,
    fallback: float = 0.0,
) -> tuple[float, dict]:
    """Compute a fill-based price PnL proxy without replacing exchange truth.

    Contract quantities are used only as allocation weights. The authoritative
    total remains the exchange realized PnL supplied by position history.
    """
    entry_qty = sum(_fill_qty(row) for row in entry_fills)
    entry_px = _weighted_average(entry_fills, ("fillPx", "fill_price", "avgPx", "price"))
    exit_qty = sum(_fill_qty(row) for row in exit_fills)
    exit_px = _weighted_average(exit_fills, ("fillPx", "fill_price", "avgPx", "price"))
    notional = max(float(entry_notional_usdt or 0.0), 0.0)
    if notional <= 0 and entry_qty > 0:
        notional = sum(_fill_qty(row) * _fill_px(row) for row in entry_fills)
    if entry_qty <= 0 or entry_px <= 0 or exit_qty <= 0 or notional <= 0:
        return round(float(fallback or 0.0), 8), {
            "entry_contracts": round(entry_qty, 12),
            "exit_contracts": round(exit_qty, 12),
            "average_entry_price": round(entry_px, 12),
            "average_exit_price": round(exit_px, 12),
            "gross_price_pnl_method": "fallback",
        }
    per_contract_notional = notional / entry_qty
    gross = 0.0
    is_long = str(direction or "").lower() in {"long", "buy"}
    for fill in exit_fills:
        qty = _fill_qty(fill)
        price = _fill_px(fill)
        if qty <= 0 or price <= 0:
            continue
        move = (price - entry_px) / entry_px if is_long else (entry_px - price) / entry_px
        gross += per_contract_notional * qty * move
    return round(gross, 8), {
        "entry_contracts": round(entry_qty, 12),
        "exit_contracts": round(exit_qty, 12),
        "average_entry_price": round(entry_px, 12),
        "average_exit_price": round(exit_px, 12),
        "gross_price_pnl_method": "weighted_fill_prices",
    }


def canonical_exit_reason(reason: str) -> str:
    value = str(reason or "").strip().lower()
    if "emergency" in value:
        return "EMERGENCY"
    if "manual" in value:
        return "MANUAL"
    if "exchange" in value or "sync" in value:
        return "EXCHANGE_PROTECTION"
    if "profit_lock" in value:
        return "PROFIT_LOCK"
    if "positive_mfe" in value:
        return "POSITIVE_MFE_GUARD"
    if "break" in value or "breakeven" in value:
        return "BREAK_EVEN"
    if "trail" in value or "tsl" in value:
        return "TRAILING"
    if "tp1" in value:
        return "TP1"
    if "tp2" in value:
        return "TP2"
    if "sl" in value or "stop" in value:
        return "HARD_SL"
    return "OTHER"


def profit_retention_metrics(
    *,
    mfe_gross_pnl_usdt: float | None,
    realized_net_pnl_usdt: float | None,
    total_fees_usdt: float = 0.0,
) -> dict:
    mfe_gross = None if mfe_gross_pnl_usdt is None else float(mfe_gross_pnl_usdt)
    realized = None if realized_net_pnl_usdt is None else float(realized_net_pnl_usdt)
    mfe_net = None if mfe_gross is None else max(mfe_gross - abs(float(total_fees_usdt or 0.0)), 0.0)
    if mfe_net is None or mfe_net <= 0 or realized is None or realized <= 0:
        retention = None
        giveback = None if mfe_net is None or realized is None else max(mfe_net - realized, 0.0)
    else:
        retention = min(max(realized / mfe_net, 0.0), 1.0) * 100.0
        giveback = max(mfe_net - realized, 0.0)
    return {
        "mfe_net_equivalent_pnl_usdt": None if mfe_net is None else round(mfe_net, 8),
        "profit_giveback_usdt": None if giveback is None else round(giveback, 8),
        "profit_retention_pct": None if retention is None else round(retention, 6),
    }


class PnLReconciler:
    def __init__(self, path: Path | str = "data/pnl_reconciliation.json"):
        self.path = Path(path)
        self._lock = RLock()
        self._records: dict[str, dict] = {}
        self._retry_task: asyncio.Task | None = None
        self._client = None
        self._load()

    def _load(self) -> None:
        try:
            if self.path.exists():
                self._records = dict(json.loads(self.path.read_text(encoding="utf-8")).get("records", {}))
        except Exception:
            self._records = {}

    def _save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps({"records": self._records}, indent=2, ensure_ascii=True), encoding="utf-8")

    def record_provisional(self, trade_id: str, **fields: Any) -> dict:
        with self._lock:
            existing = self._records.get(str(trade_id))
            if existing:
                # A repeated close callback must enrich the same durable row,
                # never reset exchange evidence or finalized accounting.
                row = dict(existing)
                for key, value in fields.items():
                    if key not in row and value is not None:
                        row[key] = value
                row["updated_at"] = time.time()
                self._records[str(trade_id)] = row
                self._save()
                return dict(row)
            local_net = _f(fields.get("local_estimated_net_pnl"))
            now = time.time()
            row = {
                "trade_id": trade_id,
                "gross_price_pnl": _f(fields.get("gross_price_pnl")),
                "entry_fee": _f(fields.get("entry_fee")),
                "exit_fee": _f(fields.get("exit_fee")),
                "funding": _f(fields.get("funding")),
                "rebates": _f(fields.get("rebates")),
                "other_adjustments": _f(fields.get("other_adjustments")),
                "exchange_realized_pnl": None,
                "local_estimated_net_pnl": local_net,
                "reconciliation_difference": None,
                "pnl_status": "PROVISIONAL",
                "exchange_confirmed_at": None,
                "created_at": now,
                "risk_applied_amount": 0.0,
                "risk_applied_source": None,
                "execution_environment": str(fields.get("execution_environment", "production") or "production"),
                "accounting_status": "PROVISIONAL",
                "exchange_final": False,
                "entry_fills": [],
                "exit_fills": [],
                "funding_bills": [],
                "total_fees_usdt": 0.0,
                "computed_net_pnl": None,
                "net_exchange_pnl": None,
                "reconciliation_status": "PROVISIONAL",
                "primary_exit_reason": canonical_exit_reason(str(fields.get("exit_reason", "") or "")),
            }
            row.update({key: value for key, value in fields.items() if key not in row})
            self._records[trade_id] = row
            self._save()
            return dict(row)

    def update_record(self, trade_id: str, **fields: Any) -> dict | None:
        """Attach later read-only observations without changing PnL authority."""
        with self._lock:
            row = self._records.get(str(trade_id))
            if not row:
                return None
            row.update(fields)
            row["updated_at"] = time.time()
            self._records[str(trade_id)] = row
            self._save()
            return dict(row)

    @staticmethod
    def _dedupe(rows: list[dict], keys: tuple[str, ...]) -> list[dict]:
        seen: set[str] = set()
        result: list[dict] = []
        for row in rows:
            identity = next((str(row.get(key)) for key in keys if row.get(key) not in {None, ""}), json.dumps(row, sort_keys=True))
            if identity in seen:
                continue
            seen.add(identity)
            result.append(row)
        return result

    def reconcile_payloads(
        self,
        trade_id: str,
        *,
        entry_fills: list[dict],
        exit_fills: list[dict],
        bills: list[dict],
        exchange_realized_pnl: float | None,
        evidence: dict[str, dict] | None = None,
        tolerance_usdt: float = 0.01,
    ) -> dict:
        with self._lock:
            row = dict(self._records.get(trade_id) or self.record_provisional(trade_id))
            entries = _dedupe_rows(entry_fills, ("fillId", "tradeId", "billId"))
            exits = _dedupe_rows(exit_fills, ("fillId", "tradeId", "billId"))
            unique_bills = _dedupe_rows(bills, ("billId", "tradeId"))
            entry_fee = sum(_f(fill.get("fee")) for fill in entries)
            exit_fee = sum(_f(fill.get("fee")) for fill in exits)
            funding_bills = [
                bill for bill in unique_bills
                if str(
                    bill.get("type")
                    or bill.get("subType")
                    or bill.get("billType")
                    or ""
                ).lower() in {"8", "173", "funding", "funding_fee"}
            ]
            funding = sum(_f(bill.get("balChg", bill.get("pnl", bill.get("amount", 0)))) for bill in funding_bills)
            rebates = sum(max(_f(fill.get("fee")), 0.0) for fill in entries + exits)
            fee_cost = sum(max(-_f(fill.get("fee")), 0.0) for fill in entries + exits)
            direction = str(row.get("direction", "long") or "long")
            computed_gross, fill_summary = _compute_gross_price_pnl(
                entries,
                exits,
                direction=direction,
                entry_notional_usdt=_f(row.get("entry_notional_usdt", row.get("size_usdt", 0.0))),
                fallback=_f(row.get("gross_price_pnl")),
            )
            total_fees = fee_cost
            computed_net = computed_gross + entry_fee + exit_fee + funding + _f(row.get("other_adjustments"))
            fee_complete = bool(
                entries
                and exits
                and all(fill.get("fee") not in {None, ""} for fill in entries + exits)
            )
            evidence_map = {
                key: _normalize_evidence((evidence or {}).get(key), source=source)
                for key, source in (
                    ("fills", "okx.fills"),
                    ("funding", "okx.bills"),
                    ("position_history", "okx.position_history"),
                    ("fees", "okx.fill_fees"),
                )
            }
            evidence_map["fees"]["fee_data_complete"] = fee_complete
            can_finalize, finalization_reasons = _can_finalize_exchange_pnl(
                entry_fills=entries,
                exit_fills=exits,
                fee_data_complete=fee_complete,
                evidence=evidence_map,
                exchange_realized_pnl=exchange_realized_pnl,
                entry_contracts=fill_summary["entry_contracts"],
                exit_contracts=fill_summary["exit_contracts"],
            )
            mfe_gross = row.get("mfe_gross_pnl_usdt")
            retention = profit_retention_metrics(
                mfe_gross_pnl_usdt=None if mfe_gross is None else _f(mfe_gross),
                realized_net_pnl_usdt=(
                    None if exchange_realized_pnl is None or not can_finalize else float(exchange_realized_pnl)
                ),
                total_fees_usdt=total_fees,
            )
            row.update({
                "entry_fills": entries,
                "exit_fills": exits,
                "funding_bills": funding_bills,
                "entry_contracts": fill_summary["entry_contracts"],
                "exit_contracts": fill_summary["exit_contracts"],
                "average_entry_price": fill_summary["average_entry_price"],
                "average_exit_price": fill_summary["average_exit_price"],
                "computed_gross_price_pnl": computed_gross,
                "gross_price_pnl": computed_gross if entries and exits else _f(row.get("gross_price_pnl")),
                "computed_net_pnl": round(computed_net, 8),
                "entry_fee": entry_fee,
                "exit_fee": exit_fee,
                "entry_fees": entry_fee,
                "exit_fees": exit_fee,
                "funding": funding,
                "rebates": rebates,
                "total_fees_usdt": round(fee_cost, 8),
                "fee_cost_usdt": round(fee_cost, 8),
                "fee_rebates_usdt": round(rebates, 8),
                "fee_data_complete": fee_complete,
                "fee_to_gross_profit_ratio": round(total_fees / computed_gross, 8) if computed_gross > 0 else None,
                "fee_bleed_classification": (
                    "MICRO_PROFIT_FEE_BLEED"
                    if computed_gross > 0 and total_fees / computed_gross >= 0.5
                    else "NONE"
                ),
                "evidence": evidence_map,
                **retention,
            })
            if exchange_realized_pnl is None:
                row.update({
                    "pnl_status": "PROVISIONAL" if not exits else "AWAITING_EXCHANGE_DATA",
                    "accounting_status": "PROVISIONAL",
                    "reconciliation_status": "AWAITING_EXCHANGE_DATA",
                })
            elif not can_finalize:
                confirmed = float(exchange_realized_pnl)
                row.update({
                    "exchange_realized_pnl": confirmed,
                    "net_exchange_pnl": confirmed,
                    "pnl_status": "AWAITING_EXCHANGE_DATA" if exits else "PROVISIONAL",
                    "accounting_status": "PROVISIONAL",
                    "exchange_confirmed_at": None,
                    "exchange_final": False,
                    "execution_environment": "production",
                    "reconciliation_status": "AWAITING_EXCHANGE_DATA",
                    "reconciliation_reason": ";".join(finalization_reasons),
                    "finalization_blockers": finalization_reasons,
                    "primary_exit_reason": canonical_exit_reason(str(row.get("exit_reason", "") or "")),
                })
            else:
                confirmed = float(exchange_realized_pnl)
                local = _f(row.get("oxypulse_reported_final_pnl", row.get("local_estimated_net_pnl")))
                difference = confirmed - local
                computed_difference = confirmed - computed_net
                status = "RECONCILED" if abs(difference) <= tolerance_usdt else ("ROUNDING_DIFFERENCE" if abs(difference) <= tolerance_usdt * 5 else "MISMATCH")
                row.update({
                    "exchange_realized_pnl": confirmed,
                    "net_exchange_pnl": confirmed,
                    "reconciliation_difference": difference,
                    "computed_vs_exchange_difference": computed_difference,
                    "pnl_status": status,
                    "accounting_status": "FINAL" if status in FINAL_STATUSES else "PROVISIONAL",
                    "exchange_confirmed_at": time.time(),
                    "exchange_final": status in FINAL_STATUSES,
                    "execution_environment": "production",
                    "gross_pnl": computed_gross,
                    "net_realized_pnl": confirmed,
                    "reconciliation_status": status,
                    "primary_exit_reason": canonical_exit_reason(str(row.get("exit_reason", "") or "")),
                })
                already_applied = (
                    _f(row.get("risk_applied_amount"))
                    if row.get("risk_applied_source") == "exchange_confirmed"
                    else 0.0
                )
                delta = confirmed - already_applied
                if abs(delta) > 1e-12:
                    try:
                        from risk_manager import risk
                        risk.record_realized_pnl(delta, source="pnl_reconciler_exchange_confirmed")
                        row["risk_applied_amount"] = confirmed
                        row["risk_applied_source"] = "exchange_confirmed"
                    except Exception:
                        pass
            self._records[trade_id] = row
            self._save()
            if row.get("exchange_final") is True and not row.get("learning_event"):
                try:
                    from exchange_final_learning import ingest
                    row["learning_event"] = ingest(row)
                    self._records[trade_id] = row
                    self._save()
                except Exception as exc:
                    row["learning_event"] = {"accepted": False, "reason": f"learning_error:{type(exc).__name__}"}
            return dict(row)

    async def reconcile_from_okx(self, client, trade_id: str, inst_id: str, order_ids: list[str]) -> dict:
        try:
            with self._lock:
                existing = dict(self._records.get(trade_id, {}))
            entry_ids = {
                str(value) for value in (
                    list(existing.get("entry_order_ids", []) or [])
                    + [existing.get("entry_order_id", "")]
                    + [existing.get("entry_client_order_id", ""), existing.get("entry_clOrdId", "")]
                ) if value
            }
            exit_ids = {
                str(value) for value in (
                    list(existing.get("exit_order_ids", []) or [])
                    + [existing.get("exit_order_id", "")]
                ) if value
            }
            supplied = {str(value) for value in (order_ids or []) if value}
            if not entry_ids or not exit_ids:
                with self._lock:
                    row = dict(existing or self.record_provisional(trade_id))
                    row["pnl_status"] = "AWAITING_EXCHANGE_DATA"
                    row["reconciliation_reason"] = "explicit_entry_and_exit_order_ids_required"
                    row["unattributed_order_ids"] = sorted(supplied)
                    self._records[trade_id] = row
                    self._save()
                    return row
            fills_evidence = await _query_source_evidence(
                client,
                status_method="get_fills_status",
                legacy_method="get_fills",
                data_key="fills",
                source="okx.fills",
                inst_id=inst_id,
            )
            fills = list(fills_evidence.get("data", []) or [])
            def _belongs(fill: dict, ids: set[str]) -> bool:
                return bool(
                    str(fill.get("ordId", "") or "") in ids
                    or str(fill.get("clOrdId", fill.get("client_order_id", "")) or "") in ids
                )

            entry_fills = [row for row in fills if _belongs(row, entry_ids)]
            exit_fills = [row for row in fills if _belongs(row, exit_ids)]
            if fills_evidence.get("status") != "SUCCESS_DATA" or not entry_fills or not exit_fills:
                with self._lock:
                    row = dict(existing)
                    row["pnl_status"] = "AWAITING_EXCHANGE_DATA"
                    row["reconciliation_reason"] = (
                        "entry_or_exit_fills_not_yet_available"
                        if fills_evidence.get("status") == "SUCCESS_DATA"
                        else f"fills_{fills_evidence.get('status', 'UNKNOWN').lower()}"
                    )
                    row["evidence"] = {"fills": fills_evidence}
                    self._records[trade_id] = row
                    self._save()
                    return row
            bills_evidence = await _query_source_evidence(
                client,
                status_method="get_bills_status",
                legacy_method="get_bills",
                data_key="bills",
                source="okx.bills",
                inst_id=inst_id,
            )
            bills = list(bills_evidence.get("data", []) or [])
            opened_at = _epoch_seconds(existing.get("first_fill_at", existing.get("opened_at", 0.0)))
            closed_at = _epoch_seconds(existing.get("last_exit_fill_at", existing.get("closed_at", 0.0)))
            episode_ids = entry_ids | exit_ids
            attributed_bills = []
            for bill in bills:
                bill_order_id = str(bill.get("ordId", bill.get("orderId", "")) or "")
                bill_ts = _epoch_seconds(bill.get("ts", bill.get("uTime", bill.get("cTime", 0.0))))
                order_match = bool(bill_order_id and bill_order_id in episode_ids)
                time_match = bool(opened_at > 0 and closed_at > 0 and opened_at - 5.0 <= bill_ts <= closed_at + 5.0)
                if order_match or time_match:
                    attributed_bills.append(bill)
            history_evidence = await _query_source_evidence(
                client,
                status_method="get_position_history_status",
                legacy_method="get_position_history",
                data_key="history",
                source="okx.position_history",
                inst_id=inst_id,
            )
            history = list(history_evidence.get("data", []) or [])
            position_id = str(existing.get("position_id", existing.get("pos_id", "")) or "")
            matched_history = [
                row for row in history
                if position_id and str(row.get("posId", "") or "") == position_id
            ]
            if not position_id and len(history) == 1:
                matched_history = list(history)
            realized = None
            if len(matched_history) == 1:
                realized_row = matched_history[0]
                for key in ("realizedPnl", "pnl"):
                    if realized_row.get(key) not in {None, ""}:
                        realized = _f(realized_row.get(key))
                        break
            history_evidence["realized_pnl_present"] = realized is not None
            fee_data_complete = bool(
                entry_fills
                and exit_fills
                and all(fill.get("fee") not in {None, ""} for fill in entry_fills + exit_fills)
            )
            return self.reconcile_payloads(
                trade_id,
                entry_fills=entry_fills,
                exit_fills=exit_fills,
                bills=attributed_bills,
                exchange_realized_pnl=realized,
                evidence={
                    "fills": fills_evidence,
                    "funding": bills_evidence,
                    "position_history": history_evidence,
                    "fees": _evidence_record(
                        status="SUCCESS_DATA" if fee_data_complete else "QUERY_FAILED",
                        data=entry_fills + exit_fills,
                        source="okx.fill_fees",
                        reason="" if fee_data_complete else "fee_fields_incomplete",
                        fee_data_complete=fee_data_complete,
                    ),
                },
            )
        except Exception:
            with self._lock:
                row = dict(self._records.get(trade_id) or self.record_provisional(trade_id))
                row["pnl_status"] = "UNRESOLVED"
                self._records[trade_id] = row
                self._save()
                return row

    def confirmed_total(self, *, since: float = 0.0) -> float:
        with self._lock:
            return sum(
                _f(row.get("exchange_realized_pnl"))
                for row in self._records.values()
                if row.get("pnl_status") in FINAL_STATUSES
                and _f(row.get("exchange_confirmed_at")) >= since
            )

    def snapshot(self) -> dict:
        with self._lock:
            statuses = {status: sum(1 for row in self._records.values() if row.get("pnl_status") == status) for status in STATUSES}
            return {"records": len(self._records), "statuses": statuses, "confirmed_total": self.confirmed_total()}

    def production_performance_snapshot(self) -> dict:
        """Return production-only PnL without blending paper or counterfactual rows."""
        with self._lock:
            rows = []
            for trade_id, stored in self._records.items():
                row = dict(stored)
                environment = str(row.get("execution_environment", "") or "").lower()
                if environment != "production" or str(trade_id).upper().startswith("PAPER-"):
                    continue
                row["trade_id"] = str(trade_id)
                rows.append(row)
        final_statuses = set(FINAL_STATUSES)
        final_rows = [row for row in rows if str(row.get("pnl_status", "")) in final_statuses]
        mismatch_rows = [row for row in rows if str(row.get("pnl_status", "")) == "MISMATCH"]
        pending_rows = [
            row for row in rows
            if str(row.get("pnl_status", "")) not in final_statuses
            and str(row.get("pnl_status", "")) != "MISMATCH"
        ]
        exchange_rows = [row for row in final_rows if row.get("exchange_realized_pnl") is not None]
        mismatch_exchange_rows = [row for row in mismatch_rows if row.get("exchange_realized_pnl") is not None]
        final_net = sum(_f(row.get("exchange_realized_pnl")) for row in exchange_rows)
        gross = sum(_f(row.get("gross_price_pnl", row.get("gross_pnl"))) for row in exchange_rows)
        fees = sum(
            _f(row.get("total_fees_usdt", abs(_f(row.get("entry_fee"))) + abs(_f(row.get("exit_fee")))))
            for row in exchange_rows
        )
        funding = sum(_f(row.get("funding")) for row in exchange_rows)
        wins = sum(1 for row in exchange_rows if _f(row.get("exchange_realized_pnl")) > 0)
        losses = sum(1 for row in exchange_rows if _f(row.get("exchange_realized_pnl")) < 0)
        neutrals = len(exchange_rows) - wins - losses
        return {
            "source": "pnl_reconciler_exchange_final",
            "generated_at": time.time(),
            "closed_final": len(final_rows),
            "reconciling": len(pending_rows) + len(mismatch_rows),
            "wins": wins,
            "losses": losses,
            "neutral": neutrals,
            "gross_pnl_usdt": round(gross, 8),
            "fees_usdt": round(fees, 8),
            "funding_usdt": round(funding, 8),
            "net_pnl_usdt": round(final_net, 8),
            "exchange_authority": "NET_EXCHANGE_PNL",
            "reconciliation_incomplete": bool(pending_rows or mismatch_rows),
            "mismatch_exchange_pnl_usdt": round(
                sum(_f(row.get("exchange_realized_pnl")) for row in mismatch_exchange_rows), 8
            ),
            "mismatch_rows_with_exchange_pnl": mismatch_exchange_rows,
            "final_rows": final_rows,
            "pending_rows": pending_rows,
            "mismatch_rows": mismatch_rows,
            "exchange_rows": exchange_rows,
        }

    async def start_retry_worker(self, client, interval_secs: float = 30.0) -> None:
        self._client = client
        if self._retry_task and not self._retry_task.done():
            return
        async def _worker() -> None:
            while True:
                try:
                    pending = [
                        dict(row) for row in self._records.values()
                        if row.get("pnl_status") in {"PROVISIONAL", "AWAITING_EXCHANGE_DATA", "UNRESOLVED"}
                    ]
                    for row in pending:
                        inst_id = str(row.get("inst_id", "") or "")
                        order_ids = [
                            str(row.get(key, "") or "")
                            for key in ("entry_order_id", "exit_order_id")
                            if row.get(key)
                        ]
                        if inst_id and order_ids:
                            await self.reconcile_from_okx(client, str(row["trade_id"]), inst_id, order_ids)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    pass
                await asyncio.sleep(max(float(interval_secs), 5.0))
        self._retry_task = asyncio.create_task(_worker(), name="pnl_reconciliation_worker")

    async def stop_retry_worker(self) -> None:
        if self._retry_task and not self._retry_task.done():
            self._retry_task.cancel()
            try:
                await self._retry_task
            except asyncio.CancelledError:
                pass
        self._retry_task = None


reconciler = PnLReconciler()

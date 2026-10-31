"""Canonical classification for OKX mutation responses."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any


MUTATION_SUCCESS = "SUCCESS"
MUTATION_DEFINITIVE_REJECTION = "DEFINITIVE_REJECTION"
MUTATION_READBACK_REQUIRED = "READBACK_REQUIRED"


def _text(value: Any) -> str:
    return str(value or "").strip()


def _items(response: dict) -> list[dict] | None:
    data = response.get("data")
    if not isinstance(data, list):
        return None
    items: list[dict] = []
    for item in data:
        if not isinstance(item, dict):
            return None
        items.append(item)
    return items


def _client_reference(item: dict) -> str:
    for key in ("algoClOrdId", "clOrdId"):
        value = _text(item.get(key))
        if value:
            return value
    return ""


def _exchange_reference(item: dict) -> str:
    for key in ("algoId", "ordId"):
        value = _text(item.get(key))
        if value:
            return value
    return ""


@dataclass(frozen=True)
class MutationClassification:
    outcome: str
    top_level_code: str = ""
    top_level_msg: str = ""
    item_s_code: str = ""
    item_s_msg: str = ""
    client_id: str = ""
    exchange_id: str = ""
    exchange_order_id: str = ""
    exchange_algo_id: str = ""
    item_count: int = 0
    evidence: dict[str, Any] | None = None
    timestamp: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    @property
    def definitive_rejection(self) -> bool:
        return self.outcome == MUTATION_DEFINITIVE_REJECTION

    def to_dict(self) -> dict[str, Any]:
        return {
            "outcome": self.outcome,
            "top_level_code": self.top_level_code,
            "top_level_msg": self.top_level_msg,
            "item_s_code": self.item_s_code,
            "item_s_msg": self.item_s_msg,
            "client_id": self.client_id,
            "exchange_id": self.exchange_id,
            "exchange_order_id": self.exchange_order_id,
            "exchange_algo_id": self.exchange_algo_id,
            "item_count": self.item_count,
            "timestamp": self.timestamp,
            "evidence": dict(self.evidence or {}),
        }


def classify_mutation_response(
    response: object,
    *,
    expected_items: int = 1,
    submitted_client_id: str = "",
) -> MutationClassification:
    """Classify top-level and item-level OKX mutation evidence.

    A top-level success is not sufficient: every returned item must be valid,
    and any non-zero ``sCode`` is a definitive business rejection. Missing or
    uncorrelatable expected items require read-only confirmation instead.
    """
    if not isinstance(response, dict):
        return MutationClassification(MUTATION_READBACK_REQUIRED, evidence={"reason": "response_not_object"})

    top_code = _text(response.get("code"))
    top_msg = _text(response.get("msg"))
    data = _items(response)
    expected = max(int(expected_items or 0), 0)

    if top_code not in {"", "0"}:
        return MutationClassification(
            MUTATION_DEFINITIVE_REJECTION,
            top_level_code=top_code,
            top_level_msg=top_msg,
            client_id=submitted_client_id,
            item_count=len(data or []),
            evidence={"source": "top_level"},
        )
    if data is None:
        return MutationClassification(
            MUTATION_READBACK_REQUIRED,
            top_level_code=top_code,
            top_level_msg=top_msg,
            evidence={"reason": "data_not_list"},
        )
    if expected and not data:
        return MutationClassification(
            MUTATION_READBACK_REQUIRED,
            top_level_code=top_code,
            top_level_msg=top_msg,
            evidence={"reason": "expected_item_missing"},
        )

    item_results: list[tuple[dict, str, str, str, str, str, str]] = []
    for item in data:
        item_code = _text(item.get("sCode", item.get("code", "")))
        item_msg = _text(item.get("sMsg", item.get("msg", "")))
        client_reference = _client_reference(item)
        exchange_order_id = _text(item.get("ordId"))
        exchange_algo_id = _text(item.get("algoId"))
        exchange_reference = exchange_algo_id or exchange_order_id
        item_results.append((
            item, item_code, item_msg, client_reference, exchange_reference,
            exchange_order_id, exchange_algo_id,
        ))
        if item_code not in {"", "0"}:
            evidence = {"source": "item", "item": dict(item)}
            return MutationClassification(
                MUTATION_DEFINITIVE_REJECTION,
                top_level_code=top_code,
                top_level_msg=top_msg,
                item_s_code=item_code,
                item_s_msg=item_msg,
                client_id=submitted_client_id or client_reference,
                exchange_id=exchange_reference,
                exchange_order_id=exchange_order_id,
                exchange_algo_id=exchange_algo_id,
                item_count=len(data),
                evidence=evidence,
            )

    if expected and len(data) != expected:
        return MutationClassification(
            MUTATION_READBACK_REQUIRED,
            top_level_code=top_code,
            top_level_msg=top_msg,
            item_count=len(data),
            client_id=submitted_client_id,
            evidence={"reason": "unexpected_item_count", "expected_items": expected},
        )

    # A successful item must be structurally attributable. Existing OKX
    # responses sometimes omit sCode, but they still return an order/algo ID.
    if expected and any(
        code in {"", "0"} and not client_reference and not exchange_reference
        for _, code, _, client_reference, exchange_reference, _, _ in item_results
    ):
        return MutationClassification(
            MUTATION_READBACK_REQUIRED,
            top_level_code=top_code,
            top_level_msg=top_msg,
            item_count=len(data),
            client_id=submitted_client_id,
            evidence={"reason": "uncorrelatable_item"},
        )

    matched_reference = ""
    if submitted_client_id:
        client_references = [
            client_reference
            for _, _, _, client_reference, _, _, _ in item_results
            if client_reference
        ]
        matching = [reference for reference in client_references if reference == submitted_client_id]
        if client_references and not matching:
            return MutationClassification(
                MUTATION_READBACK_REQUIRED,
                top_level_code=top_code,
                top_level_msg=top_msg,
                item_count=len(data),
                client_id=submitted_client_id,
                exchange_id=next(
                    (exchange_reference for _, _, _, _, exchange_reference, _, _ in item_results if exchange_reference),
                    "",
                ),
                exchange_order_id=next(
                    (order_id for _, _, _, _, _, order_id, _ in item_results if order_id),
                    "",
                ),
                exchange_algo_id=next(
                    (algo_id for _, _, _, _, _, _, algo_id in item_results if algo_id),
                    "",
                ),
                evidence={"reason": "client_id_mismatch"},
            )
        matched_reference = matching[0] if matching else ""

    exchange_reference = next(
        (exchange_id for _, _, _, _, exchange_id, _, _ in item_results if exchange_id),
        "",
    )
    exchange_order_id = next(
        (order_id for _, _, _, _, _, order_id, _ in item_results if order_id),
        "",
    )
    exchange_algo_id = next(
        (algo_id for _, _, _, _, _, _, algo_id in item_results if algo_id),
        "",
    )

    return MutationClassification(
        MUTATION_SUCCESS,
        top_level_code=top_code,
        top_level_msg=top_msg,
        client_id=submitted_client_id,
        exchange_id=exchange_reference,
        exchange_order_id=exchange_order_id,
        exchange_algo_id=exchange_algo_id,
        item_count=len(data),
        evidence={"source": "top_level_and_items"},
    )

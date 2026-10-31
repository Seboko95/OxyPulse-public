"""Read-first OKX isolated-leverage reconciliation.

Inspection is always read-only. Applying a plan requires an explicit caller
authorization flag and never arms trading or submits an order.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any, Iterable

import settings_store as _s


@dataclass(frozen=True)
class LeverageCheck:
    coin: str
    pos_side: str
    current: int
    target: int
    minimum: int
    maximum: int
    matched: bool
    reason: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def target_leverage(instrument_max: int = 0) -> int:
    low = max(int(float(_s.get_val("leverage_min", 5) or 5)), 1)
    high = max(int(float(_s.get_val("leverage_max", 15) or 15)), low)
    if instrument_max > 0:
        high = min(high, int(instrument_max))
    default = int(float(_s.get_val("leverage_default", 10) or 10))
    return max(min(default, high), low)


async def inspect(client, coins: Iterable[str]) -> dict[str, Any]:
    checks: list[LeverageCheck] = []
    low = max(int(float(_s.get_val("leverage_min", 5) or 5)), 1)
    high = max(int(float(_s.get_val("leverage_max", 15) or 15)), low)
    for coin in sorted({str(value or "").upper() for value in coins if str(value or "").strip()}):
        rows = await client.get_leverage_info(coin, margin_mode="isolated")
        by_side = {str(row.get("posSide", "") or "").lower(): row for row in rows}
        for side in ("long", "short"):
            row = by_side.get(side) or (rows[0] if len(rows) == 1 else {})
            current = int(float(row.get("lever", 0) or 0)) if row else 0
            target = target_leverage()
            checks.append(LeverageCheck(
                coin=coin, pos_side=side, current=current, target=target,
                minimum=low, maximum=high, matched=current == target,
                reason="matched" if current == target else ("missing_readback" if current <= 0 else "reconciliation_required"),
            ))
    return {
        "ok": all(row.matched for row in checks),
        "mutation_performed": False,
        "checks": [row.to_dict() for row in checks],
    }


async def apply(
    client,
    plan: dict[str, Any],
    *,
    authorized: bool = False,
    audit_path: Path | str = "data/leverage_reconciliation_audit.jsonl",
) -> dict[str, Any]:
    if not authorized:
        return {"ok": False, "mutation_performed": False, "reason": "explicit_authorization_required"}
    positions = await client.get_positions() if hasattr(client, "get_positions") else []
    open_coins = {
        str(row.get("instId", "")).upper().split("-")[0]
        for row in positions
        if float(row.get("pos", 0.0) or 0.0) != 0.0
    }
    results = []
    for row in plan.get("checks", []):
        if bool(row.get("matched", False)):
            continue
        coin = str(row.get("coin", "")).upper()
        if coin in open_coins:
            results.append({"coin": coin, "pos_side": row.get("pos_side"), "ok": False, "reason": "open_position_blocks_leverage_change"})
            continue
        result = await client.set_leverage(
            coin,
            leverage=int(row.get("target", target_leverage())),
            pos_side=str(row.get("pos_side", "long")),
        )
        code = str(result.get("code", "") if isinstance(result, dict) else "")
        if code not in {"", "0"}:
            results.append({"coin": coin, "pos_side": row.get("pos_side"), "ok": False, "reason": f"okx_rejected:{code}", "result": result})
            continue
        readback = await client.get_leverage_info(coin, margin_mode="isolated")
        side = str(row.get("pos_side", "long"))
        matching = [value for value in readback if str(value.get("posSide", "")).lower() == side.lower()]
        actual = int(float((matching[0] if matching else (readback[0] if len(readback) == 1 else {})).get("lever", 0) or 0))
        target = int(row.get("target", target_leverage()))
        results.append({
            "coin": coin,
            "pos_side": side,
            "ok": actual == target,
            "reason": "matched" if actual == target else "readback_mismatch",
            "target": target,
            "readback": actual,
            "result": result,
        })
    payload = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "ok": bool(results) and all(bool(row.get("ok")) for row in results),
        "authorized": True,
        "mutation_performed": any("result" in row for row in results),
        "results": results,
    }
    path = Path(audit_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, sort_keys=True) + "\n")
    return payload

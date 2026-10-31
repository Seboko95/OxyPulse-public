"""Evidence-driven migration of legacy paper rows out of the live registry."""
from __future__ import annotations

import json
import shutil
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


@dataclass(frozen=True)
class Classification:
    coin: str
    classification: str
    reason: str
    order_id: str
    opened_at: str
    engine: str
    position_id: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def classify_record(coin: str, row: dict[str, Any], exchange_symbols: Iterable[str]) -> Classification:
    exchange = {str(value or "").upper().split("-")[0] for value in exchange_symbols}
    order_id = str(row.get("order_id", "") or "")
    position_id = str(row.get("position_id", "") or "")
    entry_ids = [str(v or "") for v in row.get("entry_order_ids", []) or []]
    exit_ids = [str(v or "") for v in row.get("exit_order_ids", []) or []]
    production_ids = [v for v in [position_id, *entry_ids, *exit_ids] if v and not v.startswith("PAPER-")]
    coin_u = str(coin or "").upper()
    if coin_u in exchange or production_ids:
        kind = "PRODUCTION_LINKED"
        reason = "exchange symbol or production lifecycle identifier exists"
    elif order_id.startswith("PAPER-"):
        kind = "LEGACY_PAPER_POSITION"
        reason = "paper order id, no production lifecycle id, and no OKX position"
    elif str(row.get("status", "")).lower() in {"closed", "inactive"}:
        kind = "LEGACY_PAPER_CLOSED_RECORD"
        reason = "inactive local row without an exchange counterpart"
    else:
        kind = "UNRESOLVED"
        reason = "insufficient provenance to remove from active production state"
    return Classification(
        coin=coin_u,
        classification=kind,
        reason=reason,
        order_id=order_id,
        opened_at=str(row.get("opened_at", "") or ""),
        engine=str(row.get("engine", "") or ""),
        position_id=position_id,
    )


def migrate(
    *,
    registry_path: Path,
    exchange_symbols: Iterable[str],
    archive_root: Path = Path("archive"),
    history_path: Path = Path("data/paper_position_history.jsonl"),
    timestamp: str | None = None,
) -> dict[str, Any]:
    stamp = timestamp or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    registry = json.loads(registry_path.read_text(encoding="utf-8")) if registry_path.exists() else {}
    classifications = [classify_record(coin, row, exchange_symbols) for coin, row in registry.items()]
    removable = {c.coin for c in classifications if c.classification.startswith("LEGACY_PAPER_")}
    archive_dir = archive_root / f"live_registry_migration_{stamp}"
    archive_dir.mkdir(parents=True, exist_ok=False)
    if registry_path.exists():
        shutil.copy2(registry_path, archive_dir / registry_path.name)
    (archive_dir / "classification.json").write_text(
        json.dumps([c.to_dict() for c in classifications], indent=2), encoding="utf-8"
    )
    history_path.parent.mkdir(parents=True, exist_ok=True)
    with history_path.open("a", encoding="utf-8") as handle:
        for coin in sorted(removable):
            payload = {
                "migration_id": stamp,
                "migrated_at": datetime.now(timezone.utc).isoformat(),
                "execution_environment": "paper",
                "active": False,
                "classification": next(c.classification for c in classifications if c.coin == coin),
                "coin": coin,
                "position": registry[coin],
            }
            handle.write(json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n")
    retained = {coin: row for coin, row in registry.items() if coin.upper() not in removable}
    registry_path.write_text(json.dumps(retained, indent=2), encoding="utf-8")
    return {
        "migration_id": stamp,
        "archive_dir": str(archive_dir),
        "history_path": str(history_path),
        "removed_from_active": sorted(removable),
        "retained_active": sorted(retained),
        "classifications": [c.to_dict() for c in classifications],
    }

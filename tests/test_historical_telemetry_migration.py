import json
from pathlib import Path

import historical_telemetry_migration as migration


def test_reused_paper_id_is_not_promoted_to_unique_lifecycle():
    row = migration.normalize({"event": "close", "lifecycle_id": "PAPER-BUY-ACT", "pnl_usdt": 1}, source_file="x", source_line=1)
    assert row["lifecycle_id"] is None
    assert row["eligible_for_production_learning"] is False


def test_migration_preserves_unknowns_and_provenance(tmp_path: Path):
    source = tmp_path / "paper.jsonl"
    source.write_text(json.dumps({"event": "close", "pair": "ACT", "timestamp": "x"}) + "\n", encoding="utf-8")
    out = tmp_path / "normalized.jsonl"
    manifest = migration.migrate([source], output_path=out, manifest_path=tmp_path / "manifest.json")
    row = json.loads(out.read_text(encoding="utf-8"))
    assert manifest["record_count"] == 1
    assert row["candidate_id"] is None
    assert row["execution_environment"] == "paper"

import json
from pathlib import Path

import legacy_registry_migration as migration


def test_classifies_paper_without_exchange_counterpart():
    row = {"order_id": "PAPER-BUY-ACT", "engine": "RYUU", "opened_at": "x"}
    result = migration.classify_record("ACT", row, [])
    assert result.classification == "LEGACY_PAPER_POSITION"


def test_production_identifier_is_never_removed():
    row = {"order_id": "123", "position_id": "prod-pos-1"}
    result = migration.classify_record("BTC", row, [])
    assert result.classification == "PRODUCTION_LINKED"


def test_migration_archives_and_preserves_paper_history(tmp_path: Path):
    registry = tmp_path / "position_registry.json"
    registry.write_text(json.dumps({
        "ACT": {"order_id": "PAPER-BUY-ACT", "engine": "RYUU"},
        "BTC": {"order_id": "123", "position_id": "prod-pos-1"},
    }), encoding="utf-8")
    result = migration.migrate(
        registry_path=registry,
        exchange_symbols=[],
        archive_root=tmp_path / "archive",
        history_path=tmp_path / "paper_history.jsonl",
        timestamp="20260808T000000Z",
    )
    assert result["removed_from_active"] == ["ACT"]
    assert list(json.loads(registry.read_text(encoding="utf-8"))) == ["BTC"]
    history = json.loads((tmp_path / "paper_history.jsonl").read_text(encoding="utf-8"))
    assert history["coin"] == "ACT"
    assert history["execution_environment"] == "paper"
    assert (tmp_path / "archive" / "live_registry_migration_20260808T000000Z" / "position_registry.json").exists()


def test_registry_default_paths_separate_paper_and_production(monkeypatch):
    import position_registry
    monkeypatch.setenv("PAPER_TRADING", "true")
    assert position_registry._default_registry_path().name == "position_registry.paper.json"
    monkeypatch.setenv("PAPER_TRADING", "false")
    assert position_registry._default_registry_path().name == "position_registry.json"

from __future__ import annotations

import ast
import copy
import json
from pathlib import Path

import novo_trade_memory as mem


def test_append_writes_one_jsonl_line(tmp_path):
    path = tmp_path / "memory.jsonl"

    result = mem.append_memory({"symbol": "BTC", "side": "LONG"}, path=path)

    assert result["status"] == "OK"
    assert result["written"] is True
    lines = path.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 1
    assert json.loads(lines[0])["symbol"] == "BTC"


def test_append_does_not_mutate_input(tmp_path):
    path = tmp_path / "memory.jsonl"
    record = {"symbol": "ETH", "module_states": {"market_dna": {"status": "OK"}}}
    before = copy.deepcopy(record)

    mem.append_memory(record, path=path)

    assert record == before


def test_missing_timestamp_is_added(tmp_path):
    path = tmp_path / "memory.jsonl"

    mem.append_memory({"symbol": "SUI"}, path=path)
    payload = json.loads(path.read_text(encoding="utf-8").splitlines()[0])

    assert "timestamp" in payload
    assert payload["timestamp"]


def test_non_dict_record_returns_safe_error(tmp_path):
    path = tmp_path / "memory.jsonl"

    result = mem.append_memory(["bad"], path=path)

    assert result["status"] == "ERROR"
    assert result["written"] is False
    assert not path.exists()


def test_read_missing_file_returns_empty_ok(tmp_path):
    result = mem.read_recent_memory(path=tmp_path / "missing.jsonl")

    assert result == {"status": "OK", "records": [], "count": 0, "errors": []}


def test_read_skips_malformed_lines(tmp_path):
    path = tmp_path / "memory.jsonl"
    path.write_text('{"symbol":"BTC"}\n{broken\n[]\n{"symbol":"ETH"}\n', encoding="utf-8")

    result = mem.read_recent_memory(path=path)

    assert result["status"] == "OK"
    assert result["count"] == 2
    assert [r["symbol"] for r in result["records"]] == ["BTC", "ETH"]
    assert len(result["errors"]) == 2


def test_read_respects_limit(tmp_path):
    path = tmp_path / "memory.jsonl"
    for idx in range(5):
        mem.append_memory({"symbol": f"S{idx}"}, path=path)

    result = mem.read_recent_memory(limit=2, path=path)

    assert result["count"] == 2
    assert [r["symbol"] for r in result["records"]] == ["S3", "S4"]


def test_summarize_empty_missing_file_is_safe(tmp_path):
    result = mem.summarize_memory(path=tmp_path / "missing.jsonl")

    assert result["status"] == "OK"
    assert result["summary"]["total_records"] == 0
    assert result["summary"]["total_pnl"] == 0.0
    assert result["summary"]["quality_tier_counts"] == {}


def test_summarize_calculates_wins_losses_pnl_and_tier_counts(tmp_path):
    path = tmp_path / "memory.jsonl"
    mem.append_memory({"symbol": "BTC", "engine": "ENMA", "outcome": "closed", "pnl": 1.5, "quality_score": 90, "quality_tier": "ELITE"}, path=path)
    mem.append_memory({"symbol": "ETH", "engine": "RONIN", "outcome": "closed", "pnl": -0.5, "quality_score": 70, "quality_tier": "GOOD"}, path=path)
    mem.append_memory({"symbol": "BTC", "engine": "ENMA", "quality_score": 80, "quality_tier": "STRONG"}, path=path)

    result = mem.summarize_memory(path=path)
    summary = result["summary"]

    assert summary["total_records"] == 3
    assert summary["records_with_outcome"] == 2
    assert summary["win_count"] == 1
    assert summary["loss_count"] == 1
    assert summary["total_pnl"] == 1.0
    assert summary["average_quality_score"] == 80.0
    assert summary["quality_tier_counts"] == {"ELITE": 1, "GOOD": 1, "STRONG": 1}


def test_summarize_groups_best_worst_symbols_and_engines(tmp_path):
    path = tmp_path / "memory.jsonl"
    mem.append_memory({"symbol": "BTC", "engine": "ENMA", "pnl": 2.0}, path=path)
    mem.append_memory({"symbol": "ETH", "engine": "RONIN", "pnl": -1.0}, path=path)
    mem.append_memory({"symbol": "BTC", "engine": "ENMA", "pnl": 0.5}, path=path)

    summary = mem.summarize_memory(path=path)["summary"]

    assert summary["best_symbols_by_pnl"][0] == {"name": "BTC", "pnl": 2.5}
    assert summary["worst_symbols_by_pnl"][0] == {"name": "ETH", "pnl": -1.0}
    assert summary["best_engines_by_pnl"][0] == {"name": "ENMA", "pnl": 2.5}
    assert summary["worst_engines_by_pnl"][0] == {"name": "RONIN", "pnl": -1.0}


def test_custom_temp_path_works(tmp_path):
    path = tmp_path / "nested" / "memory.jsonl"

    append = mem.append_memory({"symbol": "ARB"}, path=path)
    read = mem.read_recent_memory(path=path)

    assert append["status"] == "OK"
    assert read["records"][0]["symbol"] == "ARB"


def test_forbidden_imports_are_absent():
    source_path = Path(__file__).resolve().parents[1] / "novo_trade_memory.py"
    tree = ast.parse(source_path.read_text(encoding="utf-8"))
    imported = set()

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            if node.module:
                imported.add(node.module)

    forbidden = {
        "execution_router",
        "execution_admission",
        "okx_client",
        "risk_manager",
        "sizing_engine",
        "position_registry",
        "exit_manager",
        "bot",
        "tg_panels",
        "trading_engine",
    }
    assert imported.isdisjoint(forbidden)


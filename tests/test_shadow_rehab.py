from pathlib import Path
from types import SimpleNamespace
import copy
import importlib
import json
import time


def _use_tmp_ledger(monkeypatch, tmp_path):
    import shadow_rehab_tracker as srt

    path = tmp_path / "shadow_rehab_ledger.json"
    monkeypatch.setattr(srt, "PATH", path)
    monkeypatch.setattr(srt, "_LEDGER", None)
    monkeypatch.setattr(srt, "_LEDGER_PATH", None)
    monkeypatch.setattr(srt, "_DIRTY", False)
    monkeypatch.setattr(srt, "_LAST_LOAD_TS", 0.0)
    monkeypatch.setattr(srt, "_LAST_FLUSH_TS", time.monotonic())
    srt._DIAG.update({
        "loads": 0,
        "saves": 0,
        "skipped_saves": 0,
        "update_calls": 0,
        "flush_calls": 0,
        "last_error": "",
    })
    return srt, path


def test_shadow_rehab_empty_and_corrupt_ledgers_render(monkeypatch, tmp_path):
    srt, path = _use_tmp_ledger(monkeypatch, tmp_path)
    import tg_panels as tp

    settings = SimpleNamespace(get_engine_mode=lambda engine: "shadow")
    empty = srt.get_rehab_snapshot(settings)
    assert empty["engines"]
    assert all(row["status"] == "INSUFFICIENT_DATA" for row in empty["engines"])

    text = tp.panel_shadow_rehab_text(settings)
    assert "Shadow Engine Rehab" in text
    assert "INSUFFICIENT_DATA" in text
    assert len(text) < 4096

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("{broken", encoding="utf-8")
    text = tp.panel_shadow_rehab_text(settings)
    assert "Shadow Engine Rehab" in text
    assert "INSUFFICIENT_DATA" in text


def test_scoped_shadow_signal_resolves_mfe_mae_and_ignores_active_core(monkeypatch, tmp_path):
    srt, _ = _use_tmp_ledger(monkeypatch, tmp_path)
    settings = SimpleNamespace(get_engine_mode=lambda engine: "shadow")

    srt.record_shadow_signal(
        engine="FUJIN",
        symbol="ABC",
        side="LONG",
        entry_price=100.0,
        confidence=3.0,
        quality=70.0,
        advisor_strength=6.0,
        ts=1_000.0,
    )
    srt.record_shadow_signal(
        engine="ENMA",
        symbol="ABC",
        side="LONG",
        entry_price=100.0,
        confidence=3.0,
        quality=70.0,
        ts=1_000.0,
    )
    srt.update_market_price("ABC", 105.0, high=110.0, low=95.0, ts=1_000.0 + 3_700.0)

    snap = srt.get_rehab_snapshot(settings)
    fujin = next(row for row in snap["engines"] if row["engine"] == "FUJIN")
    enma_present = [row for row in snap["engines"] if row["engine"] == "ENMA"]
    assert not enma_present
    assert fujin["post_upgrade"]["trades"] == 1
    assert fujin["post_upgrade"]["wins"] == 1
    assert fujin["post_upgrade"]["avg_mfe"] == 10.0
    assert fujin["post_upgrade"]["avg_mae"] == -5.0


def test_izanagi_short_outcome_tracks_horizons_mfe_mae(monkeypatch, tmp_path):
    srt, _ = _use_tmp_ledger(monkeypatch, tmp_path)
    import tg_panels as tp

    srt.record_izanagi_contribution(
        symbol="XYZ",
        side="SHORT",
        entry_price=100.0,
        confidence=4.2,
        quality=88.0,
        advisor_strength=7.5,
        consensus_count=4,
        ts=2_000.0,
    )
    srt.update_market_price("XYZ", 95.0, high=105.0, low=90.0, ts=2_000.0 + 3_700.0)

    snap = srt.get_izanagi_snapshot()
    assert snap["signals"] == 1
    assert snap["horizons"]["15m"]["trades"] == 1
    assert snap["horizons"]["1h"]["trades"] == 1
    assert snap["horizons"]["1h"]["wr"] == 100.0
    assert snap["horizons"]["1h"]["avg_mfe"] == 10.0
    assert snap["horizons"]["1h"]["avg_mae"] == -5.0

    text = tp.panel_izanagi_outcomes_text()
    assert "IZANAGI Outcome Tracker" in text
    assert "Advisor signals: `1`" in text
    assert len(text) < 4096


def test_telegram_diagnostics_buttons_are_read_only(monkeypatch, tmp_path):
    _use_tmp_ledger(monkeypatch, tmp_path)
    import tg_panels as tp
    import settings_store as ss

    callbacks = {
        btn.callback_data
        for kb in (tp.kb_diagnostics(ss), tp.kb_settings_cat_diagnostics(ss))
        for row in kb.inline_keyboard
        for btn in row
        if getattr(btn, "callback_data", None)
    }
    assert "diag:shadow_rehab" in callbacks
    assert "diag:izanagi_outcomes" in callbacks
    forbidden = ("eng_mode_cycle", "eng_toggle", "stg_eng_tog", "room:num:", "room:bool:")
    for callback in callbacks:
        if callback in {"diag:shadow_rehab", "diag:izanagi_outcomes"}:
            assert not callback.startswith(forbidden)


def test_ledger_loads_once_and_reuses_memory(monkeypatch, tmp_path):
    srt, path = _use_tmp_ledger(monkeypatch, tmp_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(srt._empty_ledger()), encoding="utf-8")

    srt.get_rehab_snapshot(SimpleNamespace(get_engine_mode=lambda engine: "shadow"))
    srt.get_rehab_snapshot(SimpleNamespace(get_engine_mode=lambda engine: "shadow"))

    diag = srt.get_diagnostics()
    assert diag["loads"] == 1
    assert diag["loaded"] is True


def test_market_updates_do_not_repeatedly_read_or_write_inside_flush_interval(monkeypatch, tmp_path):
    srt, _ = _use_tmp_ledger(monkeypatch, tmp_path)
    monkeypatch.setattr(srt, "FLUSH_INTERVAL_SEC", 9999.0)

    srt.record_shadow_signal(
        engine="FUJIN",
        symbol="ABC",
        side="LONG",
        entry_price=100.0,
        confidence=3.0,
        quality=70.0,
        ts=1_000.0,
    )
    for idx in range(5):
        srt.update_market_price("ABC", 101.0 + idx, high=102.0 + idx, low=99.0, ts=1_100.0 + idx)

    diag = srt.get_diagnostics()
    assert diag["loads"] == 1
    assert diag["saves"] == 0
    assert diag["update_calls"] == 5
    assert diag["dirty"] is True


def test_flush_writes_valid_json_and_never_zero_byte(monkeypatch, tmp_path):
    srt, path = _use_tmp_ledger(monkeypatch, tmp_path)
    srt.record_shadow_signal(
        engine="FUJIN",
        symbol="ABC",
        side="LONG",
        entry_price=100.0,
        confidence=3.0,
        quality=70.0,
        ts=1_000.0,
    )

    result = srt.flush(force=True)

    assert result["status"] == "OK"
    assert path.exists()
    assert path.stat().st_size > 0
    data = json.loads(path.read_text(encoding="utf-8"))
    assert data["rehab_engines"]["FUJIN"]["signals"]


def test_dirty_flag_skips_unnecessary_writes(monkeypatch, tmp_path):
    srt, _ = _use_tmp_ledger(monkeypatch, tmp_path)
    srt.record_shadow_signal(
        engine="FUJIN",
        symbol="ABC",
        side="LONG",
        entry_price=100.0,
        confidence=3.0,
        quality=70.0,
        ts=1_000.0,
    )
    assert srt.flush(force=True)["status"] == "OK"
    saves = srt.get_diagnostics()["saves"]

    assert srt.flush()["status"] == "SKIPPED"
    diag = srt.get_diagnostics()
    assert diag["saves"] == saves
    assert diag["skipped_saves"] >= 1


def test_missing_and_corrupt_ledger_are_safe_with_buffer(monkeypatch, tmp_path):
    srt, path = _use_tmp_ledger(monkeypatch, tmp_path)

    snap = srt.get_rehab_snapshot(SimpleNamespace(get_engine_mode=lambda engine: "shadow"))
    assert snap["engines"]

    monkeypatch.setattr(srt, "_LEDGER", None)
    monkeypatch.setattr(srt, "_LEDGER_PATH", None)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("{broken", encoding="utf-8")
    snap = srt.get_rehab_snapshot(SimpleNamespace(get_engine_mode=lambda engine: "shadow"))
    assert snap["engines"]
    assert srt.get_diagnostics()["loads"] >= 2


def test_public_api_compatibility_and_manual_save_wrapper(monkeypatch, tmp_path):
    srt, path = _use_tmp_ledger(monkeypatch, tmp_path)
    ledger = srt._empty_ledger()
    ledger["rehab_engines"]["FUJIN"]["signals"].append(
        srt._new_signal(
            engine="FUJIN",
            symbol="ABC",
            side="LONG",
            entry_price=100.0,
            confidence=1.0,
            quality=50.0,
            source="shadow_signal",
        )
    )

    srt._save(ledger)

    data = json.loads(path.read_text(encoding="utf-8"))
    assert len(data["rehab_engines"]["FUJIN"]["signals"]) == 1
    assert callable(srt.flush)
    assert callable(srt.get_diagnostics)


def test_large_synthetic_ledger_updates_without_extra_disk_reads(monkeypatch, tmp_path):
    srt, path = _use_tmp_ledger(monkeypatch, tmp_path)
    ledger = srt._empty_ledger()
    for i in range(350):
        ledger["rehab_engines"]["FUJIN"]["signals"].append(
            srt._new_signal(
                engine="FUJIN",
                symbol="ABC" if i % 5 == 0 else f"SYM{i}",
                side="LONG",
                entry_price=100.0,
                confidence=2.0,
                quality=60.0,
                source="shadow_signal",
                ts=1_000.0,
            )
        )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(ledger), encoding="utf-8")

    srt.update_market_price("ABC", 105.0, high=108.0, low=99.0, ts=4_700.0)
    srt.update_market_price("ABC", 106.0, high=109.0, low=98.0, ts=4_701.0)

    assert srt.get_diagnostics()["loads"] == 1
    snap = srt.get_rehab_snapshot(SimpleNamespace(get_engine_mode=lambda engine: "shadow"))
    fujin = next(row for row in snap["engines"] if row["engine"] == "FUJIN")
    assert fujin["post_upgrade"]["trades"] == 70


def test_no_trading_logic_imports_added():
    source = Path("shadow_rehab_tracker.py").read_text(encoding="utf-8")
    forbidden = [
        "execution_router",
        "execution_admission",
        "risk_manager",
        "sizing_engine",
        "position_registry",
        "okx_client",
        "exit_manager",
    ]
    for name in forbidden:
        assert name not in source

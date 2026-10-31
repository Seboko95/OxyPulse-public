import asyncio
import json
import time
from dataclasses import asdict

import candidate_expectancy_telemetry as expectancy
import market_data_stream as market
import production_research_funnel as funnel
import regime_detector as regime


def _candles(count=60, *, forming=False, start=None, base=100.0):
    start = start or int((time.time() - count * 300) * 1000)
    rows = []
    for idx in range(count):
        price = base + idx * 0.1
        confirm = "0" if forming and idx == count - 1 else "1"
        rows.append([str(start + idx * 300_000), str(price), str(price + 0.2), str(price - 0.2), str(price + 0.1), "100", "0", "0", confirm])
    return rows


def test_completed_candle_regime_excludes_forming_volume(monkeypatch):
    rows = _candles(60, forming=True)
    rows[-1][5] = "0.01"
    market._cache_5m["BTC"] = market._parse_candles(rows)
    monkeypatch.setattr(regime, "fetch_candles_5m", lambda coin: asyncio.sleep(0, result=True))
    state = asyncio.run(regime.update_regime("BTC"))
    assert state.sample_count == 59
    assert state.forming_candle_excluded is True
    assert state.readiness == "READY"


def test_missing_history_is_warming_up_unknown(monkeypatch):
    market._cache_5m["BTC"] = market._parse_candles(_candles(20))
    monkeypatch.setattr(regime, "fetch_candles_5m", lambda coin: asyncio.sleep(0, result=True))
    state = asyncio.run(regime.update_regime("BTC"))
    assert state.regime == regime.UNKNOWN
    assert state.readiness == "WARMING_UP"
    assert state.reason == "insufficient_5m_samples"


def test_stale_higher_timeframe_is_explicit():
    old = int((time.time() - 40_000) * 1000)
    market._cache_1h["BTC"] = market._parse_candles(_candles(60, start=old))
    snap = market.timeframe_snapshot("BTC", "1h", 60)
    assert snap["indicator_ready"] is False
    assert snap["reason"] == "stale_1h_candle"


def test_regime_ready_path_has_completed_btc_context(monkeypatch):
    market._cache_5m["BTC"] = market._parse_candles(_candles(60))
    monkeypatch.setattr(regime, "fetch_candles_5m", lambda coin: asyncio.sleep(0, result=True))
    state = asyncio.run(regime.update_regime("BTC"))
    assert state.regime != regime.UNKNOWN
    assert state.readiness == "READY"
    assert state.latest_candle_ts > 0


def test_funnel_separates_upstream_blocks_from_engine_evaluations(tmp_path, monkeypatch):
    event_path = tmp_path / "funnel.jsonl"
    latest_path = tmp_path / "latest.json"
    monkeypatch.setattr(funnel, "EVENT_PATH", event_path)
    monkeypatch.setattr(funnel, "LATEST_PATH", latest_path)
    readiness = {"symbols": {"BTC": {"1m": {"indicator_ready": True}}}}
    funnel.begin_scan("s1", ["BTC"], readiness, {"regime": "UNKNOWN", "readiness": "WARMING_UP"})
    funnel.record("context_offered", engine="RYUU", symbol="BTC")
    funnel.record("upstream_block", engine="RYUU", symbol="BTC", reason="btc_regime_unknown")
    row = funnel.complete_scan()
    assert row["counts"]["context_offered"] == 1
    assert row["counts"]["upstream_block"] == 1
    assert row["counts"].get("engine_evaluation", 0) == 0
    assert row["by_engine"]["RYUU"]["reasons"]["btc_regime_unknown"] == 1
    assert json.loads(event_path.read_text().splitlines()[0])["symbols_UNKNOWN"] == 1


def test_funnel_tracks_hold_signal_pre_candidate_and_candidate(tmp_path, monkeypatch):
    monkeypatch.setattr(funnel, "EVENT_PATH", tmp_path / "funnel.jsonl")
    monkeypatch.setattr(funnel, "LATEST_PATH", tmp_path / "latest.json")
    funnel.begin_scan("s2", ["BTC"], {"symbols": {"BTC": {"1m": {"indicator_ready": True}}}}, {"regime": "TRENDING", "readiness": "READY"})
    for stage in ("engine_evaluation", "raw_hold", "raw_signal", "pre_candidate", "candidate"):
        funnel.record(stage, engine="RYUU", symbol="BTC")
    row = funnel.complete_scan()
    assert all(row["counts"][stage] == 1 for stage in ("engine_evaluation", "raw_hold", "raw_signal", "pre_candidate", "candidate"))


def test_positive_case_is_research_only_and_cannot_submit(tmp_path, monkeypatch):
    monkeypatch.setattr(expectancy, "EVENT_PATH", tmp_path / "events.jsonl")
    monkeypatch.setattr(expectancy, "REJECTION_PATH", tmp_path / "reject.jsonl")
    monkeypatch.setattr(expectancy, "STATE_PATH", tmp_path / "state.json")
    monkeypatch.setattr(expectancy, "POSITIVE_CASE_PATH", tmp_path / "positive.jsonl")
    expectancy._STATE_CACHE = None
    expectancy.record_evaluation(
        symbol="BTC", direction="LONG", engine="RYUU", confidence=4,
        role="hunter", contributors=["RYUU"], quality_snapshot={"regime": "TRENDING"},
        economics={"expected_net_usdt": 1.0, "expected_gross_usdt": 2.0, "size_usdt": 100},
        disposition="admit", feature_snapshot={"planned_entry": 100, "planned_stop": 99, "tp1": 102, "tp2": 103},
    )
    case = json.loads((tmp_path / "positive.jsonl").read_text().splitlines()[0])
    assert case["research_only"] is True
    assert case["order_submitted"] is False


def test_timeframe_snapshot_exposes_counts_timestamps_and_forming_state():
    market._cache_15m["SUI"] = market._parse_candles(_candles(60, forming=True))
    snap = market.timeframe_snapshot("SUI", "15m", 55)
    assert snap["available_count"] == 59
    assert snap["oldest_timestamp"] < snap["latest_timestamp"]
    assert snap["forming_candle_present"] is True

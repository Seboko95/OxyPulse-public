import asyncio
import json
import time
from pathlib import Path
from types import SimpleNamespace

import httpx

import category_engine
import latency_telemetry
import okx_client
import trading_engine


def test_batch_timing_preserves_candidate_order_and_collects_totals(monkeypatch):
    engine = category_engine.CategoryEngine("AMATERASU")
    engine.running = True
    monkeypatch.setattr(engine, "is_enabled", lambda: True)
    monkeypatch.setattr(engine, "is_shadow_mode", lambda: False)
    monkeypatch.setattr(engine, "_candidate_pool", lambda *args, **kwargs: ["A", "B", "C", "D", "E"])
    monkeypatch.setattr(category_engine, "_is_margin_cooled_down", lambda: False)
    monkeypatch.setattr(category_engine.risk, "is_daily_limit_hit", lambda _balance: (False, ""))
    monkeypatch.setattr(category_engine._perf, "update_equity_peak", lambda _balance: None)
    monkeypatch.setattr(
        category_engine._s,
        "get_val",
        lambda key, default=None: 2 if key == "engine_eval_cooperative_batch_size" else False if key == "candidate_quality_summary_enabled" else default,
    )
    evaluated = []

    async def fake_evaluate(coin, _balance):
        evaluated.append(coin)

    engine._evaluate_coin = fake_evaluate
    collector = latency_telemetry.ScanLatencyTelemetry(1)
    token = latency_telemetry.activate(collector)
    try:
        asyncio.run(engine.evaluate_all(["A", "B", "C", "D", "E"], 500.0))
    finally:
        latency_telemetry.reset(token)

    snapshot = collector.snapshot(status="completed")
    stats = snapshot["engines"]["AMATERASU"]
    assert evaluated == ["A", "B", "C", "D", "E"]
    assert stats["batch_count"] == 3
    assert stats["total_symbols_evaluated"] == 5
    assert stats["total_evaluation_duration_sec"] >= 0.0
    assert len(stats["slowest_batches"]) == 3


def test_slow_symbol_data_is_bounded():
    collector = latency_telemetry.ScanLatencyTelemetry(1, top_n=3)
    for index in range(10):
        collector.record_symbol("AMATERASU", f"C{index}", index / 10.0)
    snapshot = collector.snapshot(status="completed")
    assert len(snapshot["slow_symbols_overall"]) == 3
    assert [row["symbol"] for row in snapshot["slow_symbols_overall"]] == ["C9", "C8", "C7"]
    assert len(snapshot["slow_symbols_by_engine"]["AMATERASU"]) == 3


def test_fresh_ws_ticker_records_ws_without_rest(monkeypatch):
    monkeypatch.setattr(
        category_engine,
        "execution_price_snapshot",
        lambda *_args, **_kwargs: {"fresh": True, "price": 10.0, "latest_timestamp": 1.0},
    )

    class NoRequest:
        async def get_ticker(self, _coin):
            raise AssertionError("REST ticker must not be called for a fresh WS price")

    collector = latency_telemetry.ScanLatencyTelemetry(1)
    token = latency_telemetry.activate(collector)
    try:
        result = asyncio.run(category_engine._candidate_preview_ticker(NoRequest(), "BTC", engine_name="AMATERASU"))
    finally:
        latency_telemetry.reset(token)
    assert result["source"] == "ws_1m"
    ticker = collector.snapshot(status="completed")["ticker"]
    assert ticker["ws_price_used"] == 1
    assert ticker["rest_fallback"] == 0


def test_rest_ticker_fallback_records_one_existing_request(monkeypatch):
    monkeypatch.setattr(
        category_engine,
        "execution_price_snapshot",
        lambda *_args, **_kwargs: {"fresh": False, "price": 0.0},
    )

    class FakeClient:
        async def get_ticker(self, _coin):
            return {"last": 11.0}

    collector = latency_telemetry.ScanLatencyTelemetry(1)
    token = latency_telemetry.activate(collector)
    try:
        result = asyncio.run(category_engine._candidate_preview_ticker(FakeClient(), "ETH", engine_name="AMATERASU"))
    finally:
        latency_telemetry.reset(token)
    assert result["source"] == "okx_rest"
    ticker = collector.snapshot(status="completed")["ticker"]
    assert ticker["rest_fallback"] == 1
    assert ticker["rest_fallback_failures"] == 0


def test_rest_telemetry_records_duration_retry_and_no_sensitive_material(monkeypatch):
    request = httpx.Request("GET", "https://example.test/api/v5/public/test")
    responses = iter([
        httpx.Response(503, request=request, json={"code": "1"}),
        httpx.Response(200, request=request, json={"code": "0", "data": []}),
    ])

    class FakeHTTP:
        async def request(self, *args, **kwargs):
            return next(responses)

    client = okx_client.OKXClient()
    monkeypatch.setattr(okx_client, "_http_client", FakeHTTP())
    monkeypatch.setattr(client, "_headers", lambda *_args: {})
    async def no_sleep(*_args, **_kwargs):
        return None

    monkeypatch.setattr(okx_client.asyncio, "sleep", no_sleep)
    collector = latency_telemetry.ScanLatencyTelemetry(1)
    token = latency_telemetry.activate(collector)
    try:
        result = asyncio.run(client._request("GET", "/api/v5/public/test"))
    finally:
        latency_telemetry.reset(token)
    assert result["code"] == "0"
    rest = collector.snapshot(status="completed")["rest"]
    assert rest["request_count"] == 1
    assert rest["retry_count"] == 1
    assert rest["timeout_count"] == 0
    assert "secret" not in json.dumps(rest).lower()
    assert "passphrase" not in json.dumps(rest).lower()


def test_rest_telemetry_failure_does_not_change_successful_request(monkeypatch):
    request = httpx.Request("GET", "https://example.test/api/v5/public/test")

    class FakeHTTP:
        async def request(self, *args, **kwargs):
            return httpx.Response(200, request=request, json={"code": "0"})

    client = okx_client.OKXClient()
    monkeypatch.setattr(okx_client, "_http_client", FakeHTTP())
    monkeypatch.setattr(client, "_headers", lambda *_args: {})
    collector = latency_telemetry.ScanLatencyTelemetry(1)
    monkeypatch.setattr(collector, "record_rest", lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("telemetry")))
    token = latency_telemetry.activate(collector)
    try:
        result = asyncio.run(client._request("GET", "/api/v5/public/test"))
    finally:
        latency_telemetry.reset(token)
    assert result["code"] == "0"


def test_rest_exception_is_preserved_when_telemetry_fails(monkeypatch):
    production_error = RuntimeError("production request failure")

    class FakeHTTP:
        async def request(self, *args, **kwargs):
            raise production_error

    client = okx_client.OKXClient()
    monkeypatch.setattr(okx_client, "_http_client", FakeHTTP())
    monkeypatch.setattr(client, "_headers", lambda *_args: {})
    collector = latency_telemetry.ScanLatencyTelemetry(1)
    monkeypatch.setattr(collector, "record_rest", lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("telemetry")))
    token = latency_telemetry.activate(collector)
    try:
        try:
            asyncio.run(client._request("GET", "/api/v5/public/test"))
        except RuntimeError as exc:
            assert exc is production_error
        else:
            raise AssertionError("production request error was not raised")
    finally:
        latency_telemetry.reset(token)


def test_execution_handoff_telemetry_failure_is_fail_open():
    collector = latency_telemetry.ScanLatencyTelemetry(1)
    original = collector.record_execution_handoff
    try:
        collector.record_execution_handoff = lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("telemetry"))
        token = latency_telemetry.activate(collector)
        latency_telemetry.record_execution_handoff(0.1)
        latency_telemetry.reset(token)
    finally:
        collector.record_execution_handoff = original
    assert collector.snapshot(status="completed")["execution_handoff"]["handoff_count"] == 0


def test_central_execution_handoff_result_survives_telemetry_failure(monkeypatch):
    import capital_reservations
    import candidate_expectancy_telemetry
    import execution_router
    import instrument_registry
    import opportunity_queue

    class Instrument:
        def notional_to_contracts(self, _size, _price):
            return 1.0

        def contracts_to_notional(self, contracts, price):
            return contracts * price

        def required_margin(self, _contracts, _price, _leverage):
            return 5.0

        def round_price(self, price, **_kwargs):
            return price

    class Router:
        async def submit(self, _request):
            return SimpleNamespace(
                ok=True,
                order_id="OID-1",
                filled_contracts=1.0,
                average_fill_price=100.0,
                protection={},
                legacy_response=lambda: {"ok": True, "ordId": "OID-1"},
            )

    async def reserve(**_kwargs):
        return True, "", "RES-1"

    monkeypatch.setattr(capital_reservations.book, "reserve", reserve)
    monkeypatch.setattr(instrument_registry.registry, "require", lambda _coin: Instrument())
    monkeypatch.setattr(execution_router, "router_for", lambda _okx: Router())
    monkeypatch.setattr(opportunity_queue.queue, "remove", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(candidate_expectancy_telemetry, "record_execution", lambda **_kwargs: None)

    collector = latency_telemetry.ScanLatencyTelemetry(1)
    monkeypatch.setattr(collector, "record_execution_handoff", lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("telemetry")))
    token = latency_telemetry.activate(collector)
    try:
        result = asyncio.run(category_engine._execute_central_entry(
            okx=SimpleNamespace(_set_last_order_skip_reason=lambda *_args: None),
            engine_name="AMATERASU",
            coin="BTC",
            side="buy",
            size_usdt=100.0,
            entry_px=100.0,
            stop_px=95.0,
            take_profit=110.0,
            leverage=10,
            allocator_meta={
                "allocator_approved_contracts": 1.0,
                "allocator_approved_risk": 1.0,
                "allocator_deployable_margin": 100.0,
                "allocator_currently_deployed_margin": 0.0,
                "allocator_required_margin_usdt": 5.0,
            },
        ))
    finally:
        latency_telemetry.reset(token)
    assert result["ok"] is True


def test_ticker_telemetry_failure_preserves_ws_and_rest_selection(monkeypatch):
    class FailingTelemetry:
        def record_ticker(self, *_args, **_kwargs):
            raise RuntimeError("telemetry")

    collector = FailingTelemetry()
    monkeypatch.setattr(latency_telemetry, "current", lambda: collector)
    monkeypatch.setattr(
        category_engine,
        "execution_price_snapshot",
        lambda *_args, **_kwargs: {"fresh": True, "price": 10.0, "latest_timestamp": 1.0},
    )

    class NoRequest:
        async def get_ticker(self, _coin):
            raise AssertionError("REST ticker must not be called")

    result = asyncio.run(category_engine._candidate_preview_ticker(NoRequest(), "BTC", engine_name="AMATERASU"))
    assert result["source"] == "ws_1m"

    monkeypatch.setattr(
        category_engine,
        "execution_price_snapshot",
        lambda *_args, **_kwargs: {"fresh": False, "price": 0.0},
    )

    class RestClient:
        async def get_ticker(self, _coin):
            return {"last": 11.0}

    result = asyncio.run(category_engine._candidate_preview_ticker(RestClient(), "BTC", engine_name="AMATERASU"))
    assert result["source"] == "okx_rest"


def test_signal_funnel_write_cost_is_observed_without_changing_write(monkeypatch, tmp_path):
    path = tmp_path / "signal_funnel_counters.json"
    monkeypatch.setattr(category_engine, "_SIGNAL_FUNNEL_COUNTER_PATH", Path(path))
    collector = latency_telemetry.ScanLatencyTelemetry(1)
    token = latency_telemetry.activate(collector)
    try:
        category_engine._save_signal_funnel_counters()
    finally:
        latency_telemetry.reset(token)
    assert path.exists()
    assert collector.snapshot(status="completed")["signal_funnel_writes"]["write_count"] == 1


def test_signal_funnel_telemetry_failure_does_not_change_write(monkeypatch, tmp_path):
    path = tmp_path / "signal_funnel_counters.json"
    monkeypatch.setattr(category_engine, "_SIGNAL_FUNNEL_COUNTER_PATH", Path(path))
    collector = latency_telemetry.ScanLatencyTelemetry(1)
    monkeypatch.setattr(collector, "record_signal_write", lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("telemetry")))
    token = latency_telemetry.activate(collector)
    try:
        category_engine._save_signal_funnel_counters()
    finally:
        latency_telemetry.reset(token)
    assert path.exists()


def test_timeout_records_duration_and_timeout_status(monkeypatch):
    async def fake_wait_for(awaitable, _timeout=None, **_kwargs):
        awaitable.close()
        raise asyncio.TimeoutError

    async def release_pending(*_args, **_kwargs):
        return []

    monkeypatch.setattr(trading_engine.asyncio, "wait_for", fake_wait_for)
    monkeypatch.setattr(trading_engine, "registry_release_uncommitted_pending", release_pending)
    monkeypatch.setattr(trading_engine._s, "get_val", lambda _key, default=None: default)

    class Engine:
        async def evaluate_all(self, _universe, _balance):
            await asyncio.sleep(0)

    runner = object.__new__(trading_engine.MultiEngine)
    collector = latency_telemetry.ScanLatencyTelemetry(1)
    token = latency_telemetry.activate(collector)
    try:
        result = asyncio.run(runner._run_engine_scan_with_timeout("AMATERASU", Engine(), [], 500.0))
    finally:
        latency_telemetry.reset(token)
    assert result["timeout"] is True
    stats = collector.snapshot(status="completed_with_engine_timeouts")["engines"]["AMATERASU"]
    assert stats["timeout"] is True
    assert stats["total_evaluation_duration_sec"] >= 0.0


def test_incomplete_scan_report_persists_bounded_diagnostic_row(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)
    runner = object.__new__(trading_engine.MultiEngine)
    runner._persist_scan_latency_report(
        {
            "cycle_number": 7,
            "scan_status": "failed",
            "duration_sec": 12.5,
            "universe_count": 282,
            "active_engine_count": 28,
            "phase_durations_sec": {"universe": 4.0},
            "engine_timeouts": ["AMATERASU"],
            "engine_errors": [],
            "engine_durations_sec": {"AMATERASU": 12.0},
            "latency_metrics": {"schema_version": 2, "scan_status": "failed", "other_unattributed_duration_sec": 8.5},
        }
    )
    latest = json.loads((tmp_path / "data" / "scan_latency_latest.json").read_text(encoding="utf-8"))
    history = (tmp_path / "data" / "scan_latency_history.jsonl").read_text(encoding="utf-8").splitlines()
    assert latest["schema_version"] == 2
    assert latest["scan_status"] == "failed"
    assert latest["latency_metrics"]["scan_status"] == "failed"
    assert len(history) == 1


def test_collector_does_not_create_per_symbol_files(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    collector = latency_telemetry.ScanLatencyTelemetry(1)
    for index in range(20):
        collector.record_symbol("AMATERASU", f"S{index}", 0.1)
    collector.snapshot(status="completed")
    assert list(tmp_path.rglob("*")) == []


def test_two_engines_record_concurrently_without_cross_attribution():
    collector = latency_telemetry.ScanLatencyTelemetry(1)

    async def record(engine):
        for index in range(50):
            latency_telemetry.record_symbol(engine, f"{engine}-{index}", index / 100.0)
            latency_telemetry.record_batch(engine, index, 1, index / 100.0, f"{engine}-{index}", index / 100.0)
            await asyncio.sleep(0)

    async def run_both():
        token = latency_telemetry.activate(collector)
        try:
            await asyncio.gather(record("A"), record("B"))
        finally:
            latency_telemetry.reset(token)

    asyncio.run(run_both())
    snapshot = collector.snapshot(status="completed")
    assert set(snapshot["engines"]) == {"A", "B"}
    assert all(
        row["engine"] == engine
        for engine in ("A", "B")
        for row in snapshot["slow_symbols_by_engine"][engine]
    )
    assert snapshot["engines"]["A"]["total_symbols_evaluated"] == 50
    assert snapshot["engines"]["B"]["total_symbols_evaluated"] == 50


def test_cross_scan_reset_and_detached_background_context():
    scan_a = latency_telemetry.ScanLatencyTelemetry(1)
    scan_b = latency_telemetry.ScanLatencyTelemetry(2)

    async def background_record():
        latency_telemetry.record_rest("/api/v5/public/funding-rate", 0.1)

    async def run_scans():
        token_a = latency_telemetry.activate(scan_a)
        try:
            latency_telemetry.record_rest("/api/v5/account/balance", 0.1)
            task = asyncio.create_task(background_record(), context=latency_telemetry.detached_context())
            await task
        finally:
            latency_telemetry.reset(token_a)

        token_b = latency_telemetry.activate(scan_b)
        try:
            latency_telemetry.record_rest("/api/v5/account/positions", 0.1)
        finally:
            latency_telemetry.reset(token_b)

    asyncio.run(run_scans())

    assert scan_a.snapshot(status="completed")["rest"]["request_count"] == 1
    assert scan_b.snapshot(status="completed")["rest"]["request_count"] == 1


def test_scan_snapshot_failure_is_fail_open(monkeypatch):
    runner = object.__new__(trading_engine.MultiEngine)
    runner._scan_cycle_count = 0
    runner._last_scan_report = {"phase_durations_sec": {}}

    async def successful_body():
        return None

    monkeypatch.setattr(runner, "_scan_cycle_impl", successful_body)
    monkeypatch.setattr(trading_engine._latency.ScanLatencyTelemetry, "snapshot", lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("telemetry")))
    monkeypatch.setattr(runner, "_persist_scan_latency_report", lambda _report: None)
    asyncio.run(runner._scan_cycle())
    assert runner._last_scan_report["scan_status"] == "completed"


def test_scan_exception_is_preserved_when_diagnostic_path_fails(monkeypatch):
    runner = object.__new__(trading_engine.MultiEngine)
    runner._scan_cycle_count = 0
    runner._last_scan_report = {"phase_durations_sec": {}}
    production_error = RuntimeError("scan failure")

    async def failing_body():
        raise production_error

    monkeypatch.setattr(runner, "_scan_cycle_impl", failing_body)
    monkeypatch.setattr(trading_engine._latency.ScanLatencyTelemetry, "snapshot", lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("telemetry")))
    monkeypatch.setattr(runner, "_persist_scan_latency_report", lambda _report: (_ for _ in ()).throw(RuntimeError("telemetry")))
    try:
        asyncio.run(runner._scan_cycle())
    except RuntimeError as exc:
        assert exc is production_error
    else:
        raise AssertionError("original scan exception was not raised")


def test_dynamic_telemetry_maps_are_hard_bounded():
    collector = latency_telemetry.ScanLatencyTelemetry(1)
    for index in range(latency_telemetry.MAX_ENGINE_KEYS + 20):
        name = f"ENGINE-{index}"
        collector.record_symbol(name, name, 1.0)
        collector.record_batch(name, index, 1, 1.0, name, 1.0)
        collector.record_engine_summary(name, 1, 1.0)
    for index in range(latency_telemetry.MAX_ENDPOINT_KEYS + 20):
        collector.record_rest(f"/dynamic/{index}", 1.0)
    snapshot = collector.snapshot(status="completed")
    assert len(snapshot["engines"]) <= latency_telemetry.MAX_ENGINE_KEYS
    assert len(snapshot["slow_symbols_by_engine"]) <= latency_telemetry.MAX_ENGINE_KEYS
    assert len(snapshot["rest"]["by_endpoint"]) <= latency_telemetry.MAX_ENDPOINT_KEYS
    assert latency_telemetry.OVERFLOW_KEY in snapshot["engines"]
    assert latency_telemetry.OVERFLOW_KEY in snapshot["rest"]["by_endpoint"]


def test_timing_snapshot_separates_additive_and_aggregate_phases():
    collector = latency_telemetry.ScanLatencyTelemetry(1)
    snapshot = collector.snapshot(
        status="completed",
        duration_sec=3.0,
        phase_durations_sec={"htf_refresh": 1.0, "market_refresh_1m": 1.0, "market_refresh": 2.0},
    )
    assert snapshot["additive_phase_durations_sec"]["htf_refresh"] == 1.0
    assert snapshot["aggregate_phase_durations_sec"]["market_refresh"] == 2.0
    assert snapshot["phase_timing_metadata"]["overlaps"]["market_refresh"] == ["htf_refresh", "market_refresh_1m"]

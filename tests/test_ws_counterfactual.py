import json

import candidate_expectancy_telemetry
import market_data_stream
from ws_stream import OKX_WS_URL, WebSocketStream


def test_candle_stream_uses_okx_business_endpoint():
    assert OKX_WS_URL.endswith("/ws/v5/business")


def test_ws_candle_advances_counterfactual_with_exchange_timestamp(monkeypatch):
    observed = []
    monkeypatch.setattr(
        candidate_expectancy_telemetry,
        "observe_price",
        lambda symbol, price, **kwargs: observed.append((symbol, price, kwargs)),
    )
    monkeypatch.setitem(
        market_data_stream._cache_1m,
        "TEST",
        {"opens": [99.0], "highs": [100.0], "lows": [98.0], "closes": [99.5], "volumes": [1.0]},
    )
    raw = json.dumps({
        "arg": {"channel": "candle1m", "instId": "TEST-USDT-SWAP"},
        "data": [["1786284000000", "99", "101", "97", "100.5", "12", "0", "0", "0"]],
    })

    WebSocketStream()._handle_message(raw)

    assert observed == [("TEST", 100.5, {"high": 101.0, "low": 97.0, "timestamp_epoch": 1786284000.0})]
    assert market_data_stream._cache_1m["TEST"]["source"] == "okx_public_websocket"
    assert market_data_stream._cache_1m["TEST"]["timeframe"] == "1m"
    assert market_data_stream._cache_1m["TEST"]["observed_at"] > 0.0


def test_ws_new_candle_appends_without_overwriting_completed_rest_bar(monkeypatch):
    monkeypatch.setattr(candidate_expectancy_telemetry, "observe_price", lambda *args, **kwargs: None)
    monkeypatch.setitem(
        market_data_stream._cache_1m,
        "SYNC",
        {
            "timestamps": [1786284000.0], "opens": [10.0], "highs": [11.0], "lows": [9.0],
            "closes": [10.5], "volumes": [100.0], "confirmed": [True],
        },
    )
    raw = json.dumps({
        "arg": {"channel": "candle1m", "instId": "SYNC-USDT-SWAP"},
        "data": [["1786284060000", "10.5", "12", "10", "11.5", "20", "0", "0", "0"]],
    })
    stream = WebSocketStream()
    stream._handle_message(raw)
    cache = market_data_stream._cache_1m["SYNC"]
    assert cache["timestamps"] == [1786284000.0, 1786284060.0]
    assert cache["closes"] == [10.5, 11.5]
    assert cache["confirmed"] == [True, False]
    assert stream.stats()["last_candle_epoch"] == 1786284060.0


def test_ws_health_records_candle_before_rest_cache_exists(monkeypatch):
    monkeypatch.setattr(candidate_expectancy_telemetry, "observe_price", lambda *args, **kwargs: None)
    market_data_stream._cache_1m.pop("COLD", None)
    raw = json.dumps({
        "arg": {"channel": "candle1m", "instId": "COLD-USDT-SWAP"},
        "data": [["1786284120000", "10", "11", "9", "10.5", "20", "0", "0", "1"]],
    })
    stream = WebSocketStream()
    stream._handle_message(raw)
    stats = stream.stats()
    assert stats["data_messages"] == 1
    assert stats["last_candle_epoch"] == 1786284120.0
    assert stats["last_confirmed_candle_epoch"] == 1786284120.0
    assert market_data_stream._cache_1m["COLD"]["closes"] == [10.5]
    assert market_data_stream._cache_1m["COLD"]["confirmed"] == [True]


def test_execution_price_snapshot_accepts_fresh_forming_candle(monkeypatch):
    now = 1_786_284_150.0
    monkeypatch.setattr(market_data_stream.time, "time", lambda: now)
    monkeypatch.setitem(
        market_data_stream._cache_1m,
        "LIVE",
        {
            "timestamps": [now - 20.0],
            "opens": [100.0], "highs": [101.0], "lows": [99.0],
            "closes": [100.5], "volumes": [12.0], "confirmed": [False],
        },
    )

    snapshot = market_data_stream.execution_price_snapshot("LIVE", max_age_sec=90.0)

    assert snapshot["fresh"] is True
    assert snapshot["price"] == 100.5
    assert snapshot["age_sec"] == 20.0


def test_execution_price_snapshot_rejects_stale_stream_price(monkeypatch):
    now = 1_786_284_300.0
    monkeypatch.setattr(market_data_stream.time, "time", lambda: now)
    monkeypatch.setitem(
        market_data_stream._cache_1m,
        "STALE",
        {
            "timestamps": [now - 120.0],
            "closes": [100.5],
            "confirmed": [False],
        },
    )

    snapshot = market_data_stream.execution_price_snapshot("STALE", max_age_sec=90.0)

    assert snapshot["fresh"] is False
    assert snapshot["reason"] == "stale_execution_price"


def test_ws_subscription_error_is_counted():
    stream = WebSocketStream()
    stream._handle_message(json.dumps({"event": "error", "code": "60018", "msg": "wrong channel"}))
    assert stream.stats()["event_messages"] == 1
    assert stream.stats()["errors"] == 1

import asyncio
import copy
import tempfile
import time
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

import live_position_truth as lpt


class Settings:
    def __init__(self, **values):
        self.values = {
            "position_sync_grace_sec": 3,
            "live_position_refresh_sec": 3,
            "live_position_refresh_jitter_sec": 0.5,
            "live_position_retry_sec": 1.5,
            "max_live_position_stale_sec": 10,
            "pnl_drift_warn_pct": 10.0,
            "pnl_drift_warn_usd": 0.05,
            "pnl_desync_jump_pct": 50.0,
            "pnl_desync_hold_sec": 3,
            "pnl_okx_truth_lock_sec": 4,
            **values,
        }

    def get_val(self, key, default=None):
        return self.values.get(key, default)


class FakeOKX:
    def __init__(self, positions=None, marks=None):
        self.positions = list(positions or [])
        self.marks = dict(marks or {})
        self.calls = 0

    async def get_positions_status(self):
        self.calls += 1
        return {"ok": True, "positions": copy.deepcopy(self.positions), "reason": ""}

    async def get_mark_price(self, coin):
        return self.marks.get(str(coin).upper())


def opened(seconds_ago: float) -> str:
    return (datetime.now(timezone.utc) - timedelta(seconds=seconds_ago)).isoformat()


class TestLivePositionTruth(unittest.TestCase):
    def setUp(self):
        lpt.reset_runtime_state()
        self._old_paper_env = lpt.os.environ.get("PAPER_TRADING")
        lpt.os.environ["PAPER_TRADING"] = "false"
        self._old_drift_log_path = lpt.DRIFT_LOG_PATH
        self._tmp = tempfile.TemporaryDirectory()
        lpt.DRIFT_LOG_PATH = Path(self._tmp.name) / "pnl_drift_log.jsonl"
        self.settings = Settings()

    def tearDown(self):
        lpt.reset_runtime_state()
        if self._old_paper_env is None:
            lpt.os.environ.pop("PAPER_TRADING", None)
        else:
            lpt.os.environ["PAPER_TRADING"] = self._old_paper_env
        lpt.DRIFT_LOG_PATH = self._old_drift_log_path
        self._tmp.cleanup()

    def test_okx_upl_preferred_and_roi_uses_margin(self):
        okx = FakeOKX([
            {
                "instId": "TON-USDT-SWAP",
                "pos": "10",
                "posSide": "long",
                "avgPx": "5.0",
                "markPx": "5.4",
                "upl": "4.2",
                "notionalUsd": "54",
                "lever": "20",
                "margin": "2.1",
            }
        ])
        registry = {"TON": {"direction": "long", "entry": 5.0, "size_usdt": 20.0, "leverage": 20}}

        snap = asyncio.run(lpt.get_live_position_snapshot(okx, registry=registry, settings=self.settings, force_refresh=True))
        row = snap["positions"]["TON"]

        self.assertEqual(row["source"], "EXCHANGE_TRUTH")
        self.assertEqual(row["unrealised_pnl"], 4.2)
        self.assertEqual(row["mark_price"], 5.4)
        self.assertAlmostEqual(row["roi_pct"], 200.0)
        self.assertEqual(row["confidence"], "HIGH")

    def test_registry_mark_estimate_is_labelled_estimate(self):
        okx = FakeOKX([], marks={"FOO": 110.0})
        registry = {"FOO": {"direction": "long", "entry": 100.0, "size_usdt": 50.0, "leverage": 10}}

        snap = asyncio.run(lpt.get_live_position_snapshot(okx, registry=registry, settings=self.settings, force_refresh=True))
        row = snap["positions"]["FOO"]

        self.assertEqual(row["source"], "ESTIMATE")
        self.assertEqual(row["status"], "REGISTRY_ONLY")
        self.assertIn("EST", row["source_label"])
        self.assertAlmostEqual(row["unrealised_pnl"], 5.0)
        self.assertEqual(row["confidence"], "MEDIUM")

    def test_new_position_syncs_then_locks_okx_truth_without_flicker(self):
        registry = {"BAR": {"direction": "long", "entry": 10.0, "size_usdt": 25.0, "leverage": 5, "opened_at": opened(1)}}
        okx = FakeOKX([], marks={"BAR": 10.1})

        first = asyncio.run(lpt.get_live_position_snapshot(okx, registry=registry, settings=self.settings, force_refresh=True))
        self.assertEqual(first["positions"]["BAR"]["status"], "SYNCING")

        okx.positions = [{
            "instId": "BAR-USDT-SWAP",
            "pos": "5",
            "posSide": "long",
            "avgPx": "10.0",
            "markPx": "10.2",
            "upl": "1.25",
            "notionalUsd": "51",
            "lever": "5",
            "margin": "10.2",
        }]
        second = asyncio.run(lpt.get_live_position_snapshot(okx, registry=registry, settings=self.settings, force_refresh=True))
        self.assertEqual(second["positions"]["BAR"]["source"], "EXCHANGE_TRUTH")

        okx.positions = []
        third = asyncio.run(lpt.get_live_position_snapshot(okx, registry=registry, settings=self.settings, force_refresh=True))
        self.assertEqual(third["positions"]["BAR"]["source"], "EXCHANGE_TRUTH")
        self.assertIn("lock active", third["positions"]["BAR"]["mismatch_state"])

    def test_registry_only_older_than_stale_window_is_stale(self):
        registry = {"OLD": {"direction": "short", "entry": 10.0, "size_usdt": 25.0, "leverage": 5, "opened_at": opened(30)}}
        okx = FakeOKX([], marks={"OLD": 9.8})

        snap = asyncio.run(lpt.get_live_position_snapshot(okx, registry=registry, settings=self.settings, force_refresh=True))

        self.assertEqual(snap["positions"]["OLD"]["status"], "STALE")
        self.assertIsNone(snap["positions"]["OLD"]["unrealised_pnl"])

    def test_paper_position_uses_latest_price_and_is_not_stale(self):
        registry = {"PAPERCOIN": {"direction": "short", "entry": 100.0, "size_usdt": 200.0, "leverage": 10, "opened_at": opened(60)}}
        okx = FakeOKX([])
        old_env = lpt.os.environ.get("PAPER_TRADING")
        old_latest = lpt._latest_price
        try:
            lpt.os.environ["PAPER_TRADING"] = "true"
            lpt._latest_price = lambda symbol: 95.0
            snap = asyncio.run(lpt.get_live_position_snapshot(okx, registry=registry, settings=self.settings, force_refresh=True))
        finally:
            lpt._latest_price = old_latest
            if old_env is None:
                lpt.os.environ.pop("PAPER_TRADING", None)
            else:
                lpt.os.environ["PAPER_TRADING"] = old_env

        row = snap["positions"]["PAPERCOIN"]
        self.assertEqual(row["source"], "PAPER")
        self.assertEqual(row["status"], "PAPER")
        self.assertEqual(row["confidence"], "HIGH")
        self.assertAlmostEqual(row["unrealised_pnl"], 10.0)
        self.assertAlmostEqual(snap["open_pnl_usdt"], 10.0)
        self.assertEqual(snap["stale_count"], 0)

    def test_paper_position_falls_back_to_mark_price_when_latest_price_missing(self):
        registry = {"PAPERCOIN": {"direction": "long", "entry": 100.0, "size_usdt": 200.0, "leverage": 10, "opened_at": opened(60)}}
        okx = FakeOKX([], marks={"PAPERCOIN": 103.0})
        old_env = lpt.os.environ.get("PAPER_TRADING")
        old_latest = lpt._latest_price
        try:
            lpt.os.environ["PAPER_TRADING"] = "true"
            lpt._latest_price = lambda symbol: None
            snap = asyncio.run(lpt.get_live_position_snapshot(okx, registry=registry, settings=self.settings, force_refresh=True))
        finally:
            lpt._latest_price = old_latest
            if old_env is None:
                lpt.os.environ.pop("PAPER_TRADING", None)
            else:
                lpt.os.environ["PAPER_TRADING"] = old_env

        row = snap["positions"]["PAPERCOIN"]
        self.assertEqual(row["source"], "PAPER")
        self.assertEqual(row["status"], "PAPER")
        self.assertAlmostEqual(row["mark_price"], 103.0)
        self.assertAlmostEqual(row["unrealised_pnl"], 6.0)

    def test_explicit_pre_submit_reservation_is_not_a_live_position(self):
        registry = {
            "PENDING": {
                "direction": "long",
                "size_usdt": 4.0,
                "entry": 0.0,
                "leverage": 10,
                "pending_reservation": True,
                "entry_lifecycle": "PRE_SUBMIT_RESERVED",
                "mutation_attempted": False,
                "order_id": "",
            }
        }
        snapshot = asyncio.run(
            lpt.get_live_position_snapshot(
                FakeOKX([]),
                registry=registry,
                settings=self.settings,
                force_refresh=True,
            )
        )

        self.assertEqual(snapshot["source"], "EXCHANGE_TRUTH")
        self.assertEqual(snapshot["positions"], {})
        self.assertEqual(snapshot["mismatch_count"], 0)
        self.assertTrue(lpt.execution_position_truth_is_safe(snapshot, self.settings))

    def test_pending_state_without_explicit_pre_submit_marker_remains_fail_closed(self):
        registry = {
            "AMBIGUOUS": {
                "direction": "long",
                "size_usdt": 4.0,
                "entry": 0.0,
                "leverage": 10,
                "pending_reservation": True,
                "order_id": "",
                "opened_at": opened(30),
            }
        }
        snapshot = asyncio.run(
            lpt.get_live_position_snapshot(
                FakeOKX([], marks={"AMBIGUOUS": 100.0}),
                registry=registry,
                settings=self.settings,
                force_refresh=True,
            )
        )

        self.assertIn("AMBIGUOUS", snapshot["positions"])
        self.assertFalse(lpt.execution_position_truth_is_safe(snapshot, self.settings))

    def test_temporary_desync_holds_previous_value(self):
        okx = FakeOKX([{
            "instId": "TON-USDT-SWAP",
            "pos": "10",
            "posSide": "long",
            "avgPx": "5.0",
            "markPx": "5.1",
            "upl": "1.0",
            "notionalUsd": "51",
            "lever": "20",
            "margin": "2.55",
        }])
        registry = {"TON": {"direction": "long", "entry": 5.0, "size_usdt": 51.0, "leverage": 20}}
        asyncio.run(lpt.get_live_position_snapshot(okx, registry=registry, settings=self.settings, force_refresh=True))

        okx.positions[0]["upl"] = "3.0"
        okx.positions[0]["markPx"] = "5.3"
        snap = asyncio.run(lpt.get_live_position_snapshot(okx, registry=registry, settings=self.settings, force_refresh=True))
        row = snap["positions"]["TON"]

        self.assertEqual(row["status"], "TEMPORARY_DESYNC")
        self.assertEqual(row["unrealised_pnl"], 1.0)
        self.assertEqual(row["confidence"], "LOW")

    def test_jitter_bounds_and_cache_shares_snapshot(self):
        values = [lpt._jitter_ttl(self.settings) for _ in range(50)]
        self.assertTrue(all(2.5 <= value <= 3.5 for value in values))

        okx = FakeOKX([{
            "instId": "ABC-USDT-SWAP",
            "pos": "1",
            "posSide": "long",
            "avgPx": "10",
            "markPx": "10",
            "upl": "0.1",
            "notionalUsd": "10",
            "lever": "10",
            "margin": "1",
        }])
        registry = {"ABC": {"direction": "long", "entry": 10.0, "size_usdt": 10.0, "leverage": 10}}
        asyncio.run(lpt.get_live_position_snapshot(okx, registry=registry, settings=self.settings, force_refresh=True))
        asyncio.run(lpt.get_live_position_snapshot(okx, registry=registry, settings=self.settings))
        self.assertEqual(okx.calls, 1)

    def test_drift_log_writes_meaningful_entries_only(self):
        okx = FakeOKX([{
            "instId": "DRIFT-USDT-SWAP",
            "pos": "1",
            "posSide": "long",
            "avgPx": "100",
            "markPx": "110",
            "upl": "20",
            "notionalUsd": "110",
            "lever": "10",
            "margin": "11",
        }])
        registry = {"DRIFT": {"direction": "long", "entry": 100.0, "size_usdt": 100.0, "leverage": 10}}
        asyncio.run(lpt.get_live_position_snapshot(okx, registry=registry, settings=self.settings, force_refresh=True))
        text = lpt.DRIFT_LOG_PATH.read_text(encoding="utf-8")
        self.assertIn('"symbol": "DRIFT"', text)


if __name__ == "__main__":
    unittest.main()

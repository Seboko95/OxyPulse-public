import engine_attribution
import exit_manager
import exit_profiles


def test_contributors_never_receive_duplicate_pnl():
    row = engine_attribution.build_attribution(
        primary_engine="RYUU",
        confirming_engines=["ENMA", "HACHIMAN", "ENMA"],
        role_contributions={"ENMA": {"role": "confirmer"}},
        baseline_probability=0.4,
        final_probability=0.46,
    )
    closed = engine_attribution.close_attribution(row, 12.5)
    assert closed["primary_engine_pnl_usdt"] == 12.5
    assert closed["contributor_pnl_total_usdt"] == 0.0
    assert closed["contributing_engines"] == ["ENMA", "HACHIMAN"]
    assert all(item["attributed_pnl_usdt"] == 0.0 for item in closed["contribution_metadata"])


def test_historical_attribution_is_migration_safe():
    closed = engine_attribution.close_attribution({"engine": "RYUU"}, -1.0)
    assert closed["primary_engine"] == "RYUU"
    assert closed["contributing_engines"] == []


def test_exit_profiles_are_lane_specific(monkeypatch):
    monkeypatch.setattr(exit_profiles._s, "get_engine_lane", lambda engine: {"RYUU": "trend", "ARASHI": "breakout", "KAGE": "mean_reversion"}[engine])
    monkeypatch.setattr(exit_profiles._s, "get_val", lambda key, default=None: default)
    assert exit_profiles.profile_for_engine("RYUU")["trail_distance"] > 1.0
    assert exit_profiles.profile_for_engine("ARASHI")["family"] == "breakout"
    assert exit_profiles.profile_for_engine("KAGE")["tp1"] < 1.0


def test_exit_plan_persists_profile(monkeypatch):
    monkeypatch.setattr(exit_manager._s, "get_val", lambda key, default=None: default)
    plan = exit_manager.register("BTC", "long", 100, 10, 1, 99, 102, 105, engine_name="RYUU")
    assert plan.exit_profile["family"] == "trend"
    exit_manager.remove("BTC")


def test_category_tracker_uses_authoritative_close_timestamp(monkeypatch, tmp_path):
    import category_tracker
    monkeypatch.setattr(category_tracker, "_PATH", tmp_path / "stats.json")
    category_tracker._stats["RYUU"].trades.clear()
    category_tracker.record_trade(
        "RYUU", "BTC", "long", 100, 101, 10, 0.1,
        opened_at="2026-01-01T00:00:00+00:00",
        closed_at="2026-01-01T00:05:00+00:00",
    )
    assert category_tracker._stats["RYUU"].trades[-1]["closed_at"] == "2026-01-01T00:05:00+00:00"
    assert category_tracker._stats["RYUU"].trades[-1]["holding_minutes"] == 5.0

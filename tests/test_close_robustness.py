import category_engine
import category_tracker
import exit_manager


def test_category_tracker_suppresses_duplicate_close(monkeypatch):
    stats_backup = category_tracker._stats
    save_backup = category_tracker._save
    warnings_backup = set(category_tracker._duplicate_close_warnings)
    try:
        category_tracker._save = lambda: None
        category_tracker._duplicate_close_warnings.clear()
        category_tracker._stats = {
            name: category_tracker.EngineStats(name=name)
            for name in category_tracker.ENGINE_NAMES
        }
        engine = category_tracker.ENGINE_NAMES[0]
        kwargs = dict(
            engine=engine,
            coin="OP",
            direction="long",
            entry=0.09016,
            exit_price=0.09002,
            size_usdt=200.0,
            pnl_usdt=-0.31,
            fee_usdt=0.04,
            vote_score=2,
            exit_reason="sl",
            opened_at="2026-07-30T06:36:05+00:00",
        )

        category_tracker.record_trade(**kwargs)
        category_tracker.record_trade(**kwargs)

        stats = category_tracker._stats[engine]
        assert len(stats.trades) == 1
        assert stats.today_losses == 1
        assert round(stats.today_pnl, 4) == -0.35
    finally:
        category_tracker._stats = stats_backup
        category_tracker._save = save_backup
        category_tracker._duplicate_close_warnings.clear()
        category_tracker._duplicate_close_warnings.update(warnings_backup)


def test_record_close_cleans_position_when_telemetry_fails(monkeypatch):
    released = []
    removed = []
    perf_records = []
    category_records = []

    engine = category_engine.CategoryEngine.__new__(category_engine.CategoryEngine)
    engine.name = "RYUU"
    engine._open = {
        "OP": {
            "side": "buy",
            "entry": 0.09016,
            "size": 200.0,
            "leverage": 10,
            "vote_score": 2,
            "opened_at": "2026-07-30T06:36:05+00:00",
            "order_id": "PAPER-OP",
            "paper_validation": {"order_id": "PAPER-OP", "pair": "OP"},
        }
    }
    engine._cooldowns = {}
    engine._failed_coins = {}
    engine._last_trade_was_win = True

    monkeypatch.setattr(category_engine, "reg_close", lambda coin: released.append(coin))
    monkeypatch.setattr(category_engine._em, "remove", lambda coin: removed.append(coin))
    monkeypatch.setattr(category_engine, "cat_record", lambda **kwargs: category_records.append(kwargs))
    monkeypatch.setattr(
        category_engine._perf,
        "record_trade",
        lambda *args, **kwargs: perf_records.append((args, kwargs)),
    )
    monkeypatch.setattr(
        category_engine._paper_validation,
        "record_close",
        lambda **kwargs: (_ for _ in ()).throw(RuntimeError("telemetry down")),
    )
    monkeypatch.setattr(category_engine, "_set_single_sl_cooldown", lambda coin: None)
    monkeypatch.setattr(category_engine, "_record_coin_loss", lambda coin: None)
    monkeypatch.setattr(category_engine, "_mark_symbol_direction_loss", lambda coin, side: None)

    category_engine.CategoryEngine._record_close(
        engine,
        "OP",
        dict(engine._open["OP"]),
        0.09002,
        -0.31,
        "sl",
    )

    assert released == ["OP"]
    assert removed == ["OP"]
    assert "OP" not in engine._open
    assert "OP" in engine._cooldowns
    assert engine._last_trade_was_win is False
    assert category_records
    assert perf_records


def test_record_close_backfills_missing_entry_telemetry(monkeypatch):
    released = []
    removed = []
    paper_records = []

    engine = category_engine.CategoryEngine.__new__(category_engine.CategoryEngine)
    engine.name = "HAYATE"
    engine._open = {
        "KAITO": {
            "side": "sell",
            "entry": 1.0761,
            "size": 70.0,
            "leverage": 10,
            "vote_score": 3,
            "opened_at": "2026-08-02T14:09:39+00:00",
            "order_id": "PAPER-SELL-KAITO",
            "stop_loss": 1.108383,
            "risk_sizing": {
                "risk_amount_usdt": 8.97,
                "raw_size_usdt": 299.0,
                "min_exec_usdt": 50.0,
                "cap_applied": True,
                "cap_source": "participation_cap_usdt",
                "sl_dist_pct": 3.0,
            },
        }
    }
    engine._cooldowns = {}
    engine._failed_coins = {}
    engine._last_trade_was_win = True

    monkeypatch.setattr(category_engine, "reg_close", lambda coin: released.append(coin))
    monkeypatch.setattr(category_engine._em, "remove", lambda coin: removed.append(coin))
    monkeypatch.setattr(category_engine, "cat_record", lambda **kwargs: None)
    monkeypatch.setattr(category_engine._perf, "record_trade", lambda *args, **kwargs: None)
    monkeypatch.setattr(category_engine._paper_validation, "record_close", lambda **kwargs: paper_records.append(kwargs))
    monkeypatch.setattr(category_engine, "_set_single_sl_cooldown", lambda coin: None)
    monkeypatch.setattr(category_engine, "_record_coin_loss", lambda coin: None)
    monkeypatch.setattr(category_engine, "_mark_symbol_direction_loss", lambda coin, side: None)
    monkeypatch.setattr(category_engine._s, "get_val", lambda key, default=None: 12.0 if key == "rule_357_max_open_risk_pct" else default)

    category_engine.CategoryEngine._record_close(
        engine,
        "KAITO",
        dict(engine._open["KAITO"]),
        1.05,
        1.69,
        "tp2_hit",
    )

    assert released == ["KAITO"]
    assert removed == ["KAITO"]
    assert paper_records
    context = paper_records[0]["context"]
    assert context["entry_row_missing"] is True
    assert context["validation_excluded"] is True
    assert context["validation_exclusion_reason"] == "missing_entry_row_pre_current_validation"
    assert context["pair"] == "KAITO"
    assert context["entry_price"] == 1.0761
    assert context["stop_price"] == 1.108383
    assert context["final_size_usdt"] == 70.0
    assert context["current_rule_357_max_open_risk_pct"] == 12.0


def test_exit_manager_backfills_missing_entry_telemetry_context(monkeypatch):
    monkeypatch.setattr(exit_manager._s, "get_val", lambda key, default=None: 12.0 if key == "rule_357_max_open_risk_pct" else default)
    plan = exit_manager.ExitPlan(
        coin="KAITO",
        direction="short",
        engine_name="HAYATE",
        entry_price=1.0761,
        size_usdt=70.0,
        atr_val=0.01,
        hard_sl=1.108383,
        tp1_price=1.0148,
        tp2_price=0.8452,
        order_id="PAPER-SELL-KAITO",
        leverage=10,
        expected_risk_usdt=8.97,
    )

    context = exit_manager._paper_validation_close_context_from_plan(plan, "KAITO")

    assert context["entry_row_missing"] is True
    assert context["validation_excluded"] is True
    assert context["pair"] == "KAITO"
    assert context["entry_price"] == 1.0761
    assert context["stop_price"] == 1.108383
    assert round(context["sl_dist_pct"], 4) == 3.0
    assert context["final_size_usdt"] == 70.0


def test_restore_position_rehydrates_exit_plan_with_tp2(monkeypatch):
    exit_manager._plans.clear()
    engine = category_engine.CategoryEngine.__new__(category_engine.CategoryEngine)
    engine.name = "HAYATE"
    engine._open = {}

    try:
        restored = category_engine.CategoryEngine.restore_position(
            engine,
            "KAITO",
            {
                "direction": "short",
                "entry": 1.0761,
                "size_usdt": 70.0,
                "leverage": 10,
                "stop_loss": 1.108383,
                "take_profit": 1.01484301,
                "tp2_price": 0.84527655,
                "atr": 0.00782203,
                "vote_score": 3.0,
                "order_id": "PAPER-SELL-KAITO",
                "expected_risk_usdt": 6.0,
                "risk_cap_usdt": 2.1,
                "risk_ceiling_action": "size_reduced",
            },
        )

        assert restored is True
        assert engine._open["KAITO"]["take_profit"] == 1.01484301
        assert engine._open["KAITO"]["tp2"] == 0.84527655
        plan = exit_manager.get_plan("KAITO")
        assert plan is not None
        assert plan.direction == "short"
        assert plan.entry_price == 1.0761
        assert plan.hard_sl == 1.108383
        assert plan.tp1_price == 1.01484301
        assert plan.tp2_price == 0.84527655
        assert plan.size_usdt == 70.0
        assert plan.engine_name == "HAYATE"
    finally:
        exit_manager._plans.clear()

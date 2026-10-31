from datetime import datetime, timezone

import performance_tracker
import risk_manager


def test_realized_loss_breach_trips_mid_session_daily_halt(monkeypatch):
    state_backup = performance_tracker._state
    save_backup = performance_tracker._save
    risk_backup = risk_manager.risk
    try:
        performance_tracker._state = performance_tracker.PerformanceState()
        performance_tracker._save = lambda: None
        test_risk = risk_manager.RiskManager()
        risk_manager.risk = test_risk

        monkeypatch.setattr(risk_manager._s, "is_daily_cap_enabled", lambda: True)
        original_get_val = risk_manager._s.get_val

        def fake_get_val(key, default=None):
            if key == "daily_loss_pct":
                return 8.0
            if key == "rule_357_enabled":
                return False
            return original_get_val(key, default)

        monkeypatch.setattr(risk_manager._s, "get_val", fake_get_val)

        initial = test_risk.validate_trade("OP", "buy", 500.0)
        assert initial["valid"] is True

        performance_tracker.record_trade(
            coin="OP",
            direction="long",
            entry=0.09016,
            exit_price=0.088,
            size_usdt=200.0,
            pnl_usdt=-41.0,
            exit_reason="sl",
            fee_usdt=0.0,
            opened_at=datetime.now(timezone.utc).isoformat(),
        )

        status = test_risk.daily_cap_status(500.0)
        assert status["halted"] is True
        assert status["open_positions_review_required"] is True
        assert status["loss_pct"] >= 8.0

        blocked = test_risk.validate_trade("ACT", "sell", 500.0)
        assert blocked["valid"] is False
        assert "Daily loss cap" in blocked["reason"]
    finally:
        performance_tracker._state = state_backup
        performance_tracker._save = save_backup
        risk_manager.risk = risk_backup

import capital_allocator
import position_registry


def _settings(monkeypatch, overrides=None):
    values = {
        "adaptive_capital_allocator_enabled": True,
        "risk_pct": 1.0,
        "position_size_cap_pct_balance": 40.0,
        "position_size_cap_usdt": 250.0,
        "min_order_usdt": 100.0,
        "adaptive_min_order_floor_enabled": True,
        "micro_sizing_enabled": True,
        "v6_min_position_size_usdt": 25.0,
        "min_order_usdt_micro": 25.0,
        "min_order_usdt_growth": 50.0,
        "min_order_usdt_pro": 100.0,
        "micro_min_order_usdt_floor": 5.0,
        "micro_margin_buffer_min_usdt": 0.5,
        "micro_margin_buffer_pct_balance": 12.0,
        "micro_margin_buffer_max_usdt": 5.0,
        "min_free_balance_buffer_usdt": 5.0,
        "v6_micro_max_balance": 100.0,
        "v6_growth_max_balance": 1000.0,
        "leverage_default": 10,
        "rule_357_enabled": True,
        "rule_357_max_open_risk_pct": 12.0,
        "min_sl_dist_pct": 1.0,
    }
    values.update(overrides or {})
    monkeypatch.setattr(capital_allocator._s, "get_val", lambda key, default=None: values.get(key, default))

    def effective_floor(balance):
        if balance < 10:
            return 5.0
        if balance < 100:
            return 25.0
        if balance < 1000:
            return 50.0
        return 100.0

    monkeypatch.setattr(capital_allocator._s, "get_effective_min_order_usdt", effective_floor)


def test_allocator_does_not_round_up_when_cap_is_below_floor(monkeypatch):
    _settings(monkeypatch)
    decision = capital_allocator.allocate_entry(
        balance_usdt=5.0,
        requested_size_usdt=5.0,
        entry_price=100.0,
        stop_price=99.0,
        leverage=10,
        open_positions={},
    )

    assert decision.allowed is False
    assert decision.final_size_usdt == 2.0
    assert "below_min_exec" in decision.reason
    assert decision.cap_source == "position_size_cap_pct_balance"


def test_allocator_allows_normal_risk_size_when_above_floor(monkeypatch):
    _settings(monkeypatch)
    decision = capital_allocator.allocate_entry(
        balance_usdt=500.0,
        requested_size_usdt=166.67,
        entry_price=100.0,
        stop_price=97.0,
        leverage=10,
        open_positions={},
    )

    assert decision.allowed is True
    assert decision.final_size_usdt == 166.67
    assert decision.cap_applied is False
    assert decision.min_exec_usdt == 50.0


def test_allocator_reduces_to_per_position_cap(monkeypatch):
    _settings(monkeypatch)
    decision = capital_allocator.allocate_entry(
        balance_usdt=500.0,
        requested_size_usdt=500.0,
        entry_price=100.0,
        stop_price=99.0,
        leverage=10,
        open_positions={},
    )

    assert decision.allowed is True
    assert decision.final_size_usdt == 200.0
    assert decision.cap_applied is True
    assert decision.cap_source == "position_size_cap_pct_balance"


def test_shadow_can_ignore_pct_cap_without_changing_production_default(monkeypatch):
    _settings(monkeypatch)
    kwargs = dict(
        balance_usdt=500.0,
        requested_size_usdt=500.0,
        entry_price=100.0,
        stop_price=99.0,
        leverage=10,
        open_positions={},
    )
    production = capital_allocator.allocate_entry(**kwargs)
    shadow = capital_allocator.allocate_entry(**kwargs, ignore_position_size_pct_cap=True)

    assert production.final_size_usdt == 200.0
    assert production.cap_source == "position_size_cap_pct_balance"
    assert shadow.final_size_usdt == 250.0
    assert shadow.cap_source == "position_size_cap_usdt"
    assert "research_shadow_ignored_position_size_cap_pct_balance" in shadow.notes


def test_allocator_blocks_candidate_inclusive_aggregate_risk(monkeypatch):
    _settings(monkeypatch, {"rule_357_max_open_risk_pct": 2.0, "position_size_cap_usdt": 500.0, "position_size_cap_pct_balance": 100.0})
    open_positions = {
        "AAA": {
            "size_usdt": 200.0,
            "entry": 100.0,
            "stop_loss": 97.5,
            "leverage": 10,
        }
    }
    decision = capital_allocator.allocate_entry(
        balance_usdt=500.0,
        requested_size_usdt=210.0,
        entry_price=100.0,
        stop_price=97.5,
        leverage=10,
        open_positions=open_positions,
    )

    assert decision.allowed is False
    assert "open_risk_cap" in decision.reason
    assert decision.projected_open_risk_pct == 2.05


def test_allocator_uses_existing_registry_shape(monkeypatch):
    _settings(monkeypatch)
    position_registry._registry.clear()
    position_registry._registry["AAA"] = {
        "size_usdt": 100.0,
        "entry": 100.0,
        "stop_loss": 95.0,
        "leverage": 10,
    }
    decision = capital_allocator.allocate_entry(
        balance_usdt=500.0,
        requested_size_usdt=50.0,
        entry_price=100.0,
        stop_price=95.0,
        leverage=10,
        open_positions=position_registry._registry,
    )

    assert decision.allowed is True
    assert decision.projected_open_risk_pct == 1.5
    position_registry._registry.clear()

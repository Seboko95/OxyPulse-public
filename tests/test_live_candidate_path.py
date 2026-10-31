import category_engine


def test_wait_dependencies_calculate_participation_cap_before_use(monkeypatch):
    monkeypatch.setattr(
        category_engine,
        "_participation_cap_window",
        lambda *args, **kwargs: (6.35, {"source": "micro_executable_floor"}),
    )

    dependencies, cap, details = category_engine._candidate_wait_dependencies(
        engine_name="ENMA",
        direction="long",
        balance=11.0,
        spread_quality=0.70,
        signal_score=3.0,
        capital_profile=None,
        soft_accept_reason="",
        soft_accept_session="",
    )

    assert cap == 6.35
    assert dependencies["risk_capacity_usdt"] == 6.35
    assert details["source"] == "micro_executable_floor"

from __future__ import annotations

from pathlib import Path

import category_engine as ce


def _settings(monkeypatch, **overrides):
    values = {
        "vote_rescue_enabled": False,
        "vote_rescue_max_vote_gap": 1.0,
        "vote_rescue_min_quality": 62.0,
        "vote_rescue_min_admission_confidence": 2.0,
        "vote_rescue_require_support": True,
        "vote_rescue_novo_min_tier": "GOOD",
        "vote_rescue_live_opener_roles": "hunter,specialist,confirmer,booster",
    }
    values.update(overrides)
    monkeypatch.setattr(ce._s, "get_val", lambda key, default=None: values.get(key, default))


def _quality(score=72.0):
    return {
        "quality_score": score,
        "selector_bonus": 0.0,
        "consensus_count": 1,
        "regime": "TRENDING",
    }


def _role(role="hunter", *, consensus_count=2, selector_score=0.0, role_consensus=True):
    return {
        "engine_role": role,
        "consensus_count": consensus_count,
        "selector_score": selector_score,
        "role_consensus": role_consensus,
        "performance": {},
    }


def _eval(**kwargs):
    params = {
        "engine_name": "RAIJIN",
        "coin": "DYDX",
        "direction": "LONG",
        "votes": 2.0,
        "min_votes": 3.0,
        "quality_snapshot": _quality(),
        "role_ctx": _role(),
        "admission_confidence": 2.0,
        "novo_result": None,
    }
    params.update(kwargs)
    return ce._evaluate_vote_rescue(**params)


def test_default_off_near_floor_signal_is_not_rescued(monkeypatch):
    _settings(monkeypatch)

    result = _eval()

    assert result["allowed"] is False
    assert result["reason"] == "LOW_VOTE_FLOOR"


def test_enabled_live_opener_with_one_vote_gap_and_support_is_rescued(monkeypatch):
    _settings(monkeypatch, vote_rescue_enabled=True)

    result = _eval()

    assert result["allowed"] is True
    assert result["reason"] == "near_floor_supported"
    assert result["vote_gap"] == 1.0
    assert "consensus_count" in result["support_source"]


def test_low_quality_candidate_does_not_rescue(monkeypatch):
    _settings(monkeypatch, vote_rescue_enabled=True)

    result = _eval(quality_snapshot=_quality(40.0), role_ctx=_role())

    assert result["allowed"] is False
    assert result["reason"] == "VOTE_RESCUE_LOW_QUALITY"


def test_no_support_does_not_rescue(monkeypatch):
    _settings(monkeypatch, vote_rescue_enabled=True)

    result = _eval(role_ctx=_role(consensus_count=1, selector_score=0.0, role_consensus=False))

    assert result["allowed"] is False
    assert result["reason"] == "VOTE_RESCUE_NO_SUPPORT"


def test_large_vote_gap_does_not_rescue(monkeypatch):
    _settings(monkeypatch, vote_rescue_enabled=True)

    result = _eval(votes=1.5, min_votes=3.0)

    assert result["allowed"] is False
    assert result["reason"] == "VOTE_RESCUE_GAP_TOO_LARGE"


def test_selector_and_shadow_roles_cannot_independently_rescue(monkeypatch):
    _settings(monkeypatch, vote_rescue_enabled=True)

    selector = _eval(engine_name="HACHIMAN", role_ctx=_role("selector"))
    shadow = _eval(engine_name="IZANAGI", role_ctx=_role("shadow_advisor"))

    assert selector["allowed"] is False
    assert selector["reason"] == "VOTE_RESCUE_ROLE_NOT_OPENER"
    assert shadow["allowed"] is False
    assert shadow["reason"] == "VOTE_RESCUE_ROLE_NOT_OPENER"


def test_novo_good_or_better_aligned_can_support_rescue(monkeypatch):
    _settings(monkeypatch, vote_rescue_enabled=True)
    novo = {
        "quality_tier": "GOOD",
        "modules": {"market_structure": {"status": "OK", "bias": "LONG"}},
    }

    result = _eval(role_ctx=_role(consensus_count=1, selector_score=0.0, role_consensus=False), novo_result=novo)

    assert result["allowed"] is True
    assert result["novo_tier"] == "GOOD"
    assert result["support_source"] == "novo_good"


def test_novo_weak_or_collecting_cannot_support_rescue(monkeypatch):
    _settings(monkeypatch, vote_rescue_enabled=True)
    weak = {
        "quality_tier": "WEAK",
        "modules": {"market_structure": {"status": "OK", "bias": "LONG"}},
    }
    collecting = {
        "quality_tier": "COLLECTING",
        "modules": {"market_structure": {"status": "OK", "bias": "LONG"}},
    }

    weak_result = _eval(role_ctx=_role(consensus_count=1, selector_score=0.0, role_consensus=False), novo_result=weak)
    collecting_result = _eval(role_ctx=_role(consensus_count=1, selector_score=0.0, role_consensus=False), novo_result=collecting)

    assert weak_result["allowed"] is False
    assert weak_result["reason"] == "VOTE_RESCUE_NO_SUPPORT"
    assert collecting_result["allowed"] is False
    assert collecting_result["reason"] == "VOTE_RESCUE_NO_SUPPORT"


def test_rescue_metadata_does_not_contain_order_sizing_or_risk_overrides(monkeypatch):
    _settings(monkeypatch, vote_rescue_enabled=True)

    result = _eval()

    forbidden = {"order", "payload", "size", "sizing", "risk", "execution", "admission_override"}
    assert result["allowed"] is True
    assert forbidden.isdisjoint(result.keys())


def test_runtime_hook_logs_traces_and_continues_to_existing_pipeline():
    source = Path("category_engine.py").read_text(encoding="utf-8")

    assert "VOTE_RESCUE_OPEN_FLOOR" in source
    assert 'decision="vote_rescue"' in source
    assert 'decision="vote_rescue_rejected"' in source
    assert 'decision="low_vote_floor"' in source
    assert "if _vote_rescue_active:" in source
    assert "elif _role_can_contribute_below_open_votes" in source
    assert source.index("if _vote_rescue_active:") < source.index("# Repeated blocked candidates are cooled down briefly")


def test_no_new_forbidden_imports_for_vote_rescue():
    source = Path("category_engine.py").read_text(encoding="utf-8")
    helper_start = source.index("def _evaluate_vote_rescue(")
    helper_end = source.index("def _role_registry_metadata", helper_start)
    helper_source = source[helper_start:helper_end]

    for token in (
        "execution_router",
        "risk_manager",
        "sizing_engine",
        "exit_manager",
        "okx_client",
        "place_market_order",
        "_place_trade",
    ):
        assert token not in helper_source

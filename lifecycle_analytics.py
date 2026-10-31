"""
OxyPulse lifecycle analytics.

Small, pure helpers for profit-lifecycle telemetry.  These helpers do not place
orders and do not mutate exchange state; callers decide how to persist or act on
their outputs.
"""
from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Iterable


@dataclass(frozen=True)
class EconomicMinimumDecision:
    executable: bool
    reason: str
    closed_notional: float
    pnl_usdt: float
    floor_usdt: float
    remaining_notional: float
    residual_meaningful: bool
    residual_reason: str

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass(frozen=True)
class ExhaustionSnapshot:
    score: float
    reasons: str
    should_tighten: bool

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass(frozen=True)
class InvalidationDecision:
    should_invalidate: bool
    invalidation_score: float
    failure_score: float
    patience_score: float
    hold_time: float
    required_hold_time: float
    final_reason: str
    evidence: dict

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass(frozen=True)
class PostInvalidationOutcome:
    classification: str
    max_missed_mfe_roi: float
    max_adverse_roi_after: float
    avoided_loss_estimate_usdt: float
    missed_opportunity_estimate_usdt: float
    samples: int

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass(frozen=True)
class SymbolDirectionCooling:
    active: bool
    loss_count: int
    confidence_bump: float
    size_multiplier: float
    cooldown_minutes: float
    reason: str
    stage: str = "none"
    require_confirmation: bool = False
    min_consensus: int = 0
    quality_floor: float = 0.0
    override_allowed: bool = False

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass(frozen=True)
class NetExpectancyDecision:
    ok: bool
    action: str
    expected_net_usdt: float
    required_net_usdt: float
    estimated_cost_usdt: float
    size_multiplier: float
    reason: str

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass(frozen=True)
class MeaningfulProfitDecision:
    should_secure: bool
    close_pct: float
    profit_usdt: float
    target_usdt: float
    runner_preserved: bool
    reason: str

    def to_dict(self) -> dict:
        return asdict(self)


def favorable_move_frac(direction: str, from_price: float, to_price: float) -> float:
    start = float(from_price or 0.0)
    end = float(to_price or 0.0)
    if start <= 0.0 or end <= 0.0:
        return 0.0
    if str(direction or "").lower() == "short":
        return max((start - end) / start, 0.0)
    return max((end - start) / start, 0.0)


def assess_early_invalidation(
    *,
    roi: float,
    mfe_roi: float,
    mae_roi: float,
    age_sec: float,
    base_hold_sec: float,
    min_hold_floor_sec: float,
    max_hold_cap_sec: float,
    failure_threshold: float,
    patience_threshold: float,
    near_tp_earned: bool = False,
    near_tp_grace_remaining_sec: float = 0.0,
    structure_failed: bool = False,
    momentum_flipped: bool = False,
    volume_failed: bool = False,
    spread_worsened: bool = False,
    rejected_entry_zone: bool = False,
    choppy_regime: bool = False,
    low_liquidity_regime: bool = False,
    trend_regime: bool = False,
    continuation_quality: float = 0.0,
    consensus_score: float = 0.0,
    repeated_symbol_losses: int = 0,
    runner_quality: float = 0.0,
    chop_strictness_mult: float = 1.0,
    trend_patience_mult: float = 1.0,
) -> InvalidationDecision:
    failure = 0.0
    patience = 0.0
    evidence: dict = {}

    adverse_roi = abs(min(float(roi or 0.0), 0.0))
    mae_abs = abs(min(float(mae_roi or 0.0), 0.0))
    mfe = max(float(mfe_roi or 0.0), 0.0)

    if adverse_roi > 0.0:
        add = min(adverse_roi / 1.8, 2.5)
        failure += add
        evidence["fast_adverse_move"] = round(add, 4)
    if mae_abs >= max(adverse_roi, 0.01) and mae_abs >= 1.2:
        add = min(mae_abs / 2.0, 2.0)
        failure += add
        evidence["mae_expansion"] = round(add, 4)
    if structure_failed:
        failure += 2.0
        evidence["structure_failed"] = 2.0
    if momentum_flipped:
        failure += 1.25
        evidence["momentum_flipped"] = 1.25
    if spread_worsened:
        failure += 0.75
        evidence["spread_liquidity_worse"] = 0.75
    if volume_failed:
        failure += 0.75
        evidence["flow_failed"] = 0.75
    if rejected_entry_zone:
        failure += 1.0
        evidence["entry_rejected"] = 1.0
    if repeated_symbol_losses > 0:
        add = min(float(repeated_symbol_losses) * 0.75, 2.25)
        failure += add
        evidence["symbol_direction_losses"] = repeated_symbol_losses
    if choppy_regime:
        failure += 0.75 * max(float(chop_strictness_mult or 1.0), 0.0)
        evidence["choppy_regime"] = True
    if low_liquidity_regime:
        failure += 0.75
        evidence["low_liquidity_regime"] = True

    if mfe > 0.0:
        add = min(mfe / 1.5, 2.5)
        patience += add
        evidence["mfe_patience"] = round(add, 4)
    if near_tp_earned:
        patience += 2.25
        evidence["near_floor_tp_earned"] = True
    if trend_regime:
        patience += 1.0 * max(float(trend_patience_mult or 1.0), 0.0)
        evidence["trend_regime"] = True
    if continuation_quality > 0.0:
        add = min(float(continuation_quality) / 2.0, 2.0)
        patience += add
        evidence["continuation_quality"] = round(float(continuation_quality), 4)
    if consensus_score >= 3.0:
        patience += 1.0
        evidence["consensus_score"] = round(float(consensus_score), 4)
    if runner_quality > 0.0:
        add = min(float(runner_quality) / 3.0, 1.25)
        patience += add
        evidence["runner_quality"] = round(float(runner_quality), 4)
    if mae_abs > 0.0 and mfe > 0.0 and mae_abs <= max(mfe * 0.75, 0.75):
        patience += 0.75
        evidence["normal_pullback"] = True

    required = float(base_hold_sec or 0.0)
    if mfe <= 0.0:
        required *= 0.75
    if choppy_regime:
        required *= 0.75
    if repeated_symbol_losses > 0:
        required *= max(0.55, 1.0 - min(repeated_symbol_losses, 3) * 0.12)
    if near_tp_earned:
        required += max(float(near_tp_grace_remaining_sec or 0.0), 0.0)
    if trend_regime or continuation_quality >= 3.0:
        required *= max(float(trend_patience_mult or 1.0), 0.0)
    required = min(max(required, float(min_hold_floor_sec or 0.0)), float(max_hold_cap_sec or required or 0.0))

    invalidation_score = round(failure - patience, 4)
    failure = round(failure, 4)
    patience = round(patience, 4)
    if float(age_sec or 0.0) < required:
        reason = "adaptive_hold_time"
        should = False
    elif near_tp_earned and near_tp_grace_remaining_sec > 0:
        reason = "near_tp_grace"
        should = False
    elif failure < float(failure_threshold or 0.0):
        reason = "failure_below_threshold"
        should = False
    elif patience >= float(patience_threshold or 0.0) and invalidation_score <= 0:
        reason = "patience_outweighs_failure"
        should = False
    elif invalidation_score <= 0:
        reason = "failure_not_clear"
        should = False
    else:
        reason = "failure_outweighs_patience"
        should = True

    return InvalidationDecision(
        should_invalidate=should,
        invalidation_score=invalidation_score,
        failure_score=failure,
        patience_score=patience,
        hold_time=round(float(age_sec or 0.0), 4),
        required_hold_time=round(required, 4),
        final_reason=reason,
        evidence=evidence,
    )


def evaluate_post_invalidation_outcome(
    *,
    direction: str,
    entry_price: float,
    invalidation_price: float,
    current_price: float,
    max_favorable_price: float,
    max_adverse_price: float,
    size_usdt: float,
) -> PostInvalidationOutcome:
    entry = float(entry_price or 0.0)
    inv = float(invalidation_price or 0.0)
    current = float(current_price or 0.0)
    max_fav = float(max_favorable_price or 0.0)
    max_adv = float(max_adverse_price or 0.0)
    size = max(float(size_usdt or 0.0), 0.0)
    if entry <= 0.0 or inv <= 0.0:
        return PostInvalidationOutcome("neutral", 0.0, 0.0, 0.0, 0.0, 0)

    missed_frac = favorable_move_frac(direction, inv, max_fav)
    if str(direction or "").lower() == "short":
        adverse_frac = max((max_adv - inv) / inv, 0.0) if inv > 0 else 0.0
        recovered_profitable = current < entry
        continued_against = max_adv > inv
    else:
        adverse_frac = max((inv - max_adv) / inv, 0.0) if inv > 0 else 0.0
        recovered_profitable = current > entry
        continued_against = max_adv < inv

    missed = round(size * missed_frac, 6)
    avoided = round(size * adverse_frac, 6)
    if recovered_profitable and missed > avoided:
        classification = "false_positive"
    elif continued_against and avoided > missed:
        classification = "good"
    else:
        classification = "neutral"
    return PostInvalidationOutcome(
        classification=classification,
        max_missed_mfe_roi=round(missed_frac * 100.0, 6),
        max_adverse_roi_after=round(adverse_frac * 100.0, 6),
        avoided_loss_estimate_usdt=avoided,
        missed_opportunity_estimate_usdt=missed,
        samples=1,
    )


def symbol_direction_cooling_state(
    *,
    loss_count: int,
    threshold: int,
    cooldown_minutes: float,
    confidence_bump: float,
    size_multiplier: float,
    false_invalidation_recovery_rate: float = 0.0,
    mild_threshold: int = 2,
    strong_threshold: int | None = None,
    cooldown_threshold: int | None = None,
    mild_confidence_bump: float | None = None,
    mild_size_multiplier: float | None = None,
    min_consensus: int = 2,
    quality_floor: float = 80.0,
    candidate_quality: float = 0.0,
    consensus_count: int = 0,
    selector_bonus: float = 0.0,
) -> SymbolDirectionCooling:
    losses = max(int(loss_count or 0), 0)
    strong = max(int(strong_threshold if strong_threshold is not None else threshold or 3), 1)
    mild = max(min(int(mild_threshold or 2), strong), 1)
    cooldown_at = max(int(cooldown_threshold if cooldown_threshold is not None else max(strong + 2, 5)), strong)
    if losses < mild:
        return SymbolDirectionCooling(False, losses, 0.0, 1.0, 0.0, "below_threshold", stage="none")

    override_allowed = bool(
        int(consensus_count or 0) >= max(int(min_consensus or 0), 1)
        and (
            float(candidate_quality or 0.0) >= float(quality_floor or 0.0)
            or float(selector_bonus or 0.0) > 0.0
        )
    )
    recovery = max(min(float(false_invalidation_recovery_rate or 0.0), 1.0), 0.0)

    if losses >= cooldown_at:
        stage = "cooldown"
        bump_base = max(float(confidence_bump or 0.0), float(mild_confidence_bump or 0.0))
        mult_base = float(size_multiplier if size_multiplier is not None else 1.0)
        cd = max(float(cooldown_minutes or 0.0), 0.0)
        require_confirmation = True
    elif losses >= strong:
        stage = "confirm"
        bump_base = max(float(confidence_bump or 0.0), float(mild_confidence_bump or 0.0))
        mult_base = float(size_multiplier if size_multiplier is not None else 1.0)
        cd = 0.0
        require_confirmation = True
    else:
        stage = "mild"
        default_mild_bump = max(float(confidence_bump or 0.0) * 0.5, 0.25)
        bump_base = float(mild_confidence_bump if mild_confidence_bump is not None else default_mild_bump)
        default_mild_mult = max(min(float(size_multiplier if size_multiplier is not None else 1.0), 1.0), 0.05)
        default_mild_mult = max(default_mild_mult, 0.90)
        mult_base = float(mild_size_multiplier if mild_size_multiplier is not None else default_mild_mult)
        cd = 0.0
        require_confirmation = False

    bump = max(float(bump_base or 0.0) * (1.0 - recovery * 0.5), 0.0)
    mult = max(min(float(mult_base if mult_base is not None else 1.0), 1.0), 0.05)
    return SymbolDirectionCooling(
        True,
        losses,
        round(bump, 4),
        round(mult, 4),
        cd,
        "localized_symbol_direction_losses",
        stage=stage,
        require_confirmation=require_confirmation,
        min_consensus=max(int(min_consensus or 0), 0),
        quality_floor=max(float(quality_floor or 0.0), 0.0),
        override_allowed=override_allowed,
    )


def assess_net_expectancy(
    *,
    size_usdt: float,
    target_move_pct: float,
    fee_rate: float,
    spread_pct: float = 0.0,
    slippage_pct: float = 0.0,
    repeat_losses: int = 0,
    confidence: float = 0.0,
    candidate_quality: float = 0.0,
    engine_net: float = 0.0,
    degraded_data: bool = False,
    strong_quality_floor: float = 80.0,
    min_cost_multiple: float = 2.0,
    weak_cost_multiple: float = 3.0,
    soft_size_multiplier: float = 0.75,
) -> NetExpectancyDecision:
    size = max(float(size_usdt or 0.0), 0.0)
    if size <= 0.0:
        return NetExpectancyDecision(True, "accept", 0.0, 0.0, 0.0, 1.0, "no_size")
    fee = max(float(fee_rate or 0.0), 0.0)
    spread = max(float(spread_pct or 0.0), 0.0) / 100.0
    slippage = max(float(slippage_pct or 0.0), 0.0) / 100.0
    target = max(float(target_move_pct or 0.0), 0.0) / 100.0
    estimated_cost = size * ((fee * 2.0) + spread + slippage)
    expected_gross = size * target
    expected_net = expected_gross - estimated_cost

    repeat = int(repeat_losses or 0)
    weak_context = bool(
        repeat >= 2
        or bool(degraded_data)
        or float(confidence or 0.0) < 3.5
        or float(engine_net or 0.0) < 0.0
    )
    strong_setup = bool(
        float(candidate_quality or 0.0) >= float(strong_quality_floor or 0.0)
        and float(confidence or 0.0) >= 4.5
        and not bool(degraded_data)
    )
    mult = float(weak_cost_multiple if weak_context else min_cost_multiple)
    if strong_setup:
        mult = min(mult, float(min_cost_multiple or 2.0))
    required = estimated_cost * max(mult, 0.0)
    if expected_net + 1e-9 >= required:
        return NetExpectancyDecision(True, "accept", round(expected_net, 6), round(required, 6), round(estimated_cost, 6), 1.0, "edge_ok")
    if weak_context and not strong_setup:
        return NetExpectancyDecision(
            False,
            "reduce",
            round(expected_net, 6),
            round(required, 6),
            round(estimated_cost, 6),
            max(min(float(soft_size_multiplier or 1.0), 1.0), 0.05),
            "weak_net_expectancy",
        )
    return NetExpectancyDecision(True, "accept", round(expected_net, 6), round(required, 6), round(estimated_cost, 6), 1.0, "strong_or_clean_setup")


def meaningful_profit_protection(
    *,
    size_usdt: float,
    initial_size_usdt: float,
    profit_usdt: float,
    estimated_fee_usdt: float,
    min_profit_floor_usdt: float,
    runner_quality: float,
    continuation_score: float,
    consensus_score: float,
    candidate_quality: float,
    remaining_pct: float,
    runner_min_remaining_pct: float,
    base_close_pct: float,
    max_close_pct: float,
) -> MeaningfulProfitDecision:
    live_size = max(float(size_usdt or 0.0), 0.0)
    initial = max(float(initial_size_usdt or 0.0), live_size)
    profit = max(float(profit_usdt or 0.0), 0.0)
    if live_size <= 0.0 or profit <= 0.0:
        return MeaningfulProfitDecision(False, 0.0, round(profit, 6), 0.0, False, "no_profit")

    fee_floor = max(float(estimated_fee_usdt or 0.0) * 8.0, 0.0)
    base_floor = max(float(min_profit_floor_usdt or 0.0), fee_floor, initial * 0.006)
    quality = (
        float(runner_quality or 0.0)
        + float(continuation_score or 0.0)
        + min(float(consensus_score or 0.0), 5.0) * 0.25
        + (1.0 if float(candidate_quality or 0.0) >= 80.0 else 0.0)
    )
    # Strong continuation should breathe, but still secure profit if open PnL
    # materially clears costs and floor.
    target = base_floor * (1.35 if quality >= 4.0 else 1.15)
    if profit + 1e-9 < target:
        return MeaningfulProfitDecision(False, 0.0, round(profit, 6), round(target, 6), False, "below_meaningful_target")

    runner_floor = max(min(float(runner_min_remaining_pct or 0.0), 95.0), 0.0)
    current_remaining = max(min(float(remaining_pct or 100.0), 100.0), 0.0)
    available_to_close = max(current_remaining - runner_floor, 0.0)
    requested = max(float(base_close_pct or 0.0), 0.0)
    cap = max(float(max_close_pct or requested or 0.0), 0.0)
    close_pct = min(max(requested, 0.0), cap, available_to_close)
    if close_pct <= 0.0:
        return MeaningfulProfitDecision(False, 0.0, round(profit, 6), round(target, 6), True, "runner_floor_preserved")
    return MeaningfulProfitDecision(
        True,
        round(close_pct, 4),
        round(profit, 6),
        round(target, 6),
        bool((current_remaining - close_pct) >= runner_floor),
        "meaningful_profit_secure",
    )


def evaluate_economic_minimum(
    *,
    current_notional: float,
    close_pct: float,
    pnl_usdt: float,
    floor_usdt: float,
    min_remaining_notional: float,
    continuation: bool = False,
) -> EconomicMinimumDecision:
    pct = min(max(float(close_pct or 0.0), 0.0), 100.0)
    current = max(float(current_notional or 0.0), 0.0)
    closed = round(current * pct / 100.0, 8) if pct > 0.0 else 0.0
    remaining = round(max(current - closed, 0.0), 8)
    floor = max(float(floor_usdt or 0.0), 0.0)
    pnl = float(pnl_usdt or 0.0)
    min_remaining = max(float(min_remaining_notional or 0.0), 0.0)

    residual_meaningful = remaining >= min_remaining or (bool(continuation) and remaining > 0.0)
    if remaining <= 0.0:
        residual_reason = "no_residual"
    elif remaining < min_remaining and continuation:
        residual_reason = "continuation_lottery_runner"
    elif remaining < min_remaining:
        residual_reason = "below_economic_minimum"
    else:
        residual_reason = "meaningful"

    if pct <= 0.0:
        return EconomicMinimumDecision(False, "close_pct_zero", closed, pnl, floor, remaining, residual_meaningful, residual_reason)
    if closed <= 0.0:
        return EconomicMinimumDecision(False, "closed_notional_zero", closed, pnl, floor, remaining, residual_meaningful, residual_reason)
    if pnl <= 0.0:
        return EconomicMinimumDecision(False, "pnl_not_positive", closed, pnl, floor, remaining, residual_meaningful, residual_reason)
    if pnl < floor:
        return EconomicMinimumDecision(False, "below_profit_floor", closed, pnl, floor, remaining, residual_meaningful, residual_reason)
    if not residual_meaningful and remaining > 0.0:
        return EconomicMinimumDecision(False, "residual_below_economic_minimum", closed, pnl, floor, remaining, residual_meaningful, residual_reason)
    return EconomicMinimumDecision(True, "executable", closed, pnl, floor, remaining, residual_meaningful, residual_reason)


def update_runner_opportunity(
    current: dict | None,
    *,
    direction: str,
    stage: str,
    trim_pct: float,
    stage_price: float,
    current_price: float,
    initial_notional: float,
) -> dict:
    data = dict(current or {})
    stage_key = str(stage or "").lower()
    if stage_key not in {"tp1", "tp2", "trail"}:
        stage_key = "runner"

    trimmed_notional = max(float(initial_notional or 0.0), 0.0) * max(float(trim_pct or 0.0), 0.0) / 100.0
    move_frac = favorable_move_frac(direction, stage_price, current_price)
    opportunity = round(trimmed_notional * move_frac, 6)

    prefix = f"{stage_key}_"
    data[prefix + "trimmed_pct"] = round(max(float(trim_pct or 0.0), 0.0), 4)
    data[prefix + "stage_price"] = round(float(stage_price or 0.0), 8)
    data[prefix + "max_continuation_frac"] = round(max(float(data.get(prefix + "max_continuation_frac", 0.0) or 0.0), move_frac), 8)
    data[prefix + "opportunity_lost_usdt"] = round(max(float(data.get(prefix + "opportunity_lost_usdt", 0.0) or 0.0), opportunity), 6)
    data["runner_opportunity_lost_usdt"] = round(
        max(float(data.get("tp1_opportunity_lost_usdt", 0.0) or 0.0), 0.0)
        + max(float(data.get("tp2_opportunity_lost_usdt", 0.0) or 0.0), 0.0)
        + max(float(data.get("trail_opportunity_lost_usdt", 0.0) or 0.0), 0.0),
        6,
    )
    return data


def exhaustion_score(
    *,
    direction: str,
    closes: Iterable[float] | None,
    volumes: Iterable[float] | None = None,
    current_price: float = 0.0,
    peak_price: float = 0.0,
    continuation_score: float = 0.0,
    threshold: float = 3.0,
) -> ExhaustionSnapshot:
    prices = [float(x) for x in list(closes or []) if float(x or 0.0) > 0.0]
    vols = [float(x) for x in list(volumes or []) if float(x or 0.0) >= 0.0]
    is_short = str(direction or "").lower() == "short"
    score = 0.0
    reasons: list[str] = []

    if len(prices) >= 8:
        recent = prices[-1] - prices[-4]
        prior = prices[-4] - prices[-8]
        if is_short:
            recent = -recent
            prior = -prior
        if prior > 0 and recent <= prior * 0.25:
            score += 1.0
            reasons.append("momentum_decay")
        if recent < 0:
            score += 1.0
            reasons.append("failed_impulse")

        recent_range = max(prices[-4:]) - min(prices[-4:])
        prior_range = max(prices[-8:-4]) - min(prices[-8:-4])
        if prior_range > 0 and recent_range <= prior_range * 0.55:
            score += 0.75
            reasons.append("volatility_contraction")

    if vols and len(vols) >= 8:
        recent_vol = sum(vols[-3:]) / 3.0
        base_vol = sum(vols[-8:-3]) / 5.0
        if base_vol > 0 and recent_vol < base_vol * 0.70:
            score += 0.75
            reasons.append("volume_fade")

    price = float(current_price or 0.0)
    peak = float(peak_price or 0.0)
    if price > 0 and peak > 0:
        giveback = favorable_move_frac("short" if is_short else "long", price, peak)
        # For shorts, peak_price stores the best low; invert the simple giveback measure.
        if is_short:
            giveback = max((price - peak) / price, 0.0)
        if giveback >= 0.004:
            score += 0.75
            reasons.append("giveback")

    if float(continuation_score or 0.0) >= 3.0:
        score = max(score - 0.75, 0.0)
        reasons.append("continuation_offset")

    score = round(score, 3)
    return ExhaustionSnapshot(score=score, reasons=",".join(reasons) or "none", should_tighten=score >= float(threshold or 0.0))


def build_confidence_stage_trace(
    *,
    base_confidence: float,
    consensus_lift: float = 0.0,
    regime_modifier: float = 0.0,
    symbol_memory_modifier: float = 0.0,
    degraded_data_modifier: float = 0.0,
    recovery_adjustment: float = 0.0,
    admission_confidence: float | None = None,
) -> dict:
    base = float(base_confidence or 0.0)
    consensus = float(consensus_lift or 0.0)
    regime = float(regime_modifier or 0.0)
    symbol = float(symbol_memory_modifier or 0.0)
    degraded = float(degraded_data_modifier or 0.0)
    recovery = float(recovery_adjustment or 0.0)
    final = base + consensus + regime + symbol + degraded + recovery
    if admission_confidence is not None:
        final = float(admission_confidence or 0.0)
    return {
        "base_confidence": round(base, 4),
        "consensus_lift": round(consensus, 4),
        "regime_modifier": round(regime, 4),
        "symbol_memory_modifier": round(symbol, 4),
        "degraded_data_modifier": round(degraded, 4),
        "recovery_adjustment": round(recovery, 4),
        "admission_confidence": round(final, 4),
    }

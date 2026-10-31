"""
OxyPulse V6 / V6.1 — confidence_engine.py
Multi-factor weighted confidence scoring for a single engine's vote signal.

V6.1 upgrades over V6:
  - Multi-factor model (not just votes × weight)
  - Regime multiplier: trending boosts, ranging penalises
  - Engine performance multiplier: from live category_tracker win rate
  - MTF alignment boost: when multiple timeframes agree
  - Opposition penalty: reduces score for counter-signals
  - Capital profile threshold: MICRO/GROWTH/PRO use different bars

Score formula:
  raw_score  = raw_votes × engine_weight
  adj_score  = raw_score × regime_mult × perf_mult × vol_adj
  final      = adj_score + mtf_boost - opposition_penalty
  approved   = final >= capital_profile.confidence_threshold

All adjustments are bounded so the score cannot go negative or explode.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Optional

import settings_store as _ss

logger = logging.getLogger("OxyPulse.V6.Confidence")

_MAX_RAW_VOTES = 3.0

# Regime constants copied from regime_detector to avoid circular import
_REGIME_TRENDING = "TRENDING"
_REGIME_RANGING  = "RANGING"
_REGIME_DEAD     = "DEAD"


@dataclass
class ConfidenceResult:
    """Structured result from the V6.1 confidence scoring pass."""
    engine: str
    direction: str
    raw_votes: float
    engine_weight: float
    confidence_score: float         # final multi-factor score
    min_threshold: float            # threshold used for this evaluation
    approved: bool
    reason: str
    # V6.1 factor breakdown
    regime_mult: float = 1.0
    perf_mult: float = 1.0
    vol_adj: float = 1.0
    mtf_boost: float = 0.0
    opposition_penalty: float = 0.0
    details: dict = field(default_factory=dict)


def get_engine_weight(engine_name: str) -> float:
    """Return live (possibly adapted) weight for *engine_name*."""
    key = f"v6_weight_{engine_name.lower()}"
    setting_weight = float(_ss.get_val(key, 1.0))
    try:
        import engine_weights as _ew
        live = _ew.get_weight(engine_name)
        return live if live is not None else setting_weight
    except Exception:
        return setting_weight


def _regime_multiplier(regime: str | None) -> float:
    """Compute regime-based confidence adjustment."""
    if regime is None:
        return 1.0
    trend_boost   = float(_ss.get_val("v6_regime_trend_boost",   0.15))
    ranging_pen   = float(_ss.get_val("v6_regime_ranging_penalty", 0.10))
    if _REGIME_TRENDING in str(regime).upper():
        return 1.0 + trend_boost
    if _REGIME_RANGING in str(regime).upper():
        return 1.0 - ranging_pen
    return 1.0


def _engine_perf_multiplier(engine_name: str) -> float:
    """Compute multiplier from engine's recent win rate via category_tracker."""
    try:
        import category_tracker as _ct
        stats = _ct.get_engine_stats()
        eng   = stats.get(engine_name.upper(), {})
        wr    = float(eng.get("win_rate", 0.5))
        trades = int(eng.get("total_trades", 0))
        if trades < 5:
            return 1.0   # not enough data — neutral
        # WR 60%+ → 1.1×; WR 40%- → 0.9×; linear between
        if wr >= 0.60:
            return min(1.0 + (wr - 0.60) * 0.5, 1.25)   # max +25%
        if wr <= 0.40:
            return max(0.75, 1.0 - (0.40 - wr) * 0.5)   # min −25%
        return 1.0
    except Exception:
        return 1.0


def _volatility_adjustment(atr_ratio: float | None) -> float:
    """Reduce confidence score for high-volatility environments."""
    if atr_ratio is None or atr_ratio <= 0:
        return 1.0
    high = float(_ss.get_val("v6_vol_high_threshold_atr_pct", 0.04))
    low  = float(_ss.get_val("v6_vol_low_threshold_atr_pct",  0.01))
    if atr_ratio >= high:
        return 0.80   # high vol → −20%
    if atr_ratio <= low:
        return 1.0    # low vol → no penalty (favour cleaner setups)
    # Linear interpolation between low and high
    frac = (atr_ratio - low) / (high - low)
    return 1.0 - frac * 0.20


def _mtf_alignment_boost(mtf_aligned: bool) -> float:
    """Return a boost when multiple timeframes agree with the trade direction."""
    if mtf_aligned:
        return float(_ss.get_val("v6_mtf_align_boost", 0.20))
    return 0.0


def _opposition_penalty(opposition_votes: float) -> float:
    """Reduce confidence for each unit of opposing vote strength."""
    if opposition_votes <= 0:
        return 0.0
    factor = float(_ss.get_val("v6_opposition_penalty_factor", 0.25))
    return opposition_votes * factor


def score_signal(
    engine_name: str,
    direction: str,
    raw_votes: float,
    engine_weight: Optional[float] = None,
    regime: Optional[str] = None,
    atr_ratio: Optional[float] = None,       # atr / price (e.g. 0.02 = 2%)
    mtf_aligned: bool = False,               # True if 2+ TFs agree with direction
    opposition_votes: float = 0.0,           # estimated opposing signal strength
    capital_profile=None,                    # CapitalProfile from capital_manager
) -> ConfidenceResult:
    """Compute multi-factor confidence for a single engine's vote.

    Args:
        engine_name:      Uppercase engine name (e.g. "TITAN").
        direction:        "LONG", "SHORT", or "HOLD".
        raw_votes:        Float vote score (0.0–3.0) from evaluate().
        engine_weight:    Override weight; if None read from settings/engine_weights.
        regime:           Current market regime string ("TRENDING", "RANGING", etc.).
        atr_ratio:        atr/price; used for volatility adjustment.
        mtf_aligned:      True if 2+ extra timeframes confirm direction.
        opposition_votes: Estimated opposing votes (0 = not provided).
        capital_profile:  CapitalProfile from capital_manager.get_profile().

    Returns:
        ConfidenceResult with multi-factor breakdown and approval status.
    """
    if direction == "HOLD" or raw_votes <= 0:
        return ConfidenceResult(
            engine=engine_name, direction=direction, raw_votes=raw_votes,
            engine_weight=engine_weight or 1.0, confidence_score=0.0,
            min_threshold=0.0, approved=False, reason="HOLD or zero votes",
        )

    weight = (engine_weight if engine_weight is not None
              else get_engine_weight(engine_name))
    weight = float(weight)

    # ── Multi-factor score computation ──────────────────────────────────────
    regime_mult  = _regime_multiplier(regime)
    perf_mult    = _engine_perf_multiplier(engine_name)
    vol_adj      = _volatility_adjustment(atr_ratio)
    mtf_boost    = _mtf_alignment_boost(mtf_aligned)
    opp_penalty  = _opposition_penalty(opposition_votes)

    raw_score    = raw_votes * weight
    adj_score    = raw_score * regime_mult * perf_mult * vol_adj
    final_score  = max(0.0, adj_score + mtf_boost - opp_penalty)

    # ── Threshold: capital-profile-aware ───────────────────────────────────
    if capital_profile is not None:
        threshold = float(capital_profile.confidence_threshold)
    elif direction == "LONG":
        threshold = float(_ss.get_val("v6_min_long_confidence", 1.8))
    else:
        threshold = float(_ss.get_val("v6_min_short_confidence", 1.8))

    approved = final_score >= threshold

    reason = (
        f"score {final_score:.2f} >= threshold {threshold:.2f} ✓"
        if approved
        else (
            f"score {final_score:.2f} < threshold {threshold:.2f} "
            f"(raw={raw_votes:.1f}×w{weight:.2f} "
            f"regime×{regime_mult:.2f} perf×{perf_mult:.2f} "
            f"vol×{vol_adj:.2f} mtf+{mtf_boost:.2f} opp−{opp_penalty:.2f})"
        )
    )

    details = {
        "raw_votes": raw_votes,
        "engine_weight": weight,
        "raw_score": round(raw_score, 3),
        "regime_mult": round(regime_mult, 3),
        "perf_mult": round(perf_mult, 3),
        "vol_adj": round(vol_adj, 3),
        "mtf_boost": round(mtf_boost, 3),
        "opposition_penalty": round(opp_penalty, 3),
        "final_score": round(final_score, 3),
        "threshold": threshold,
    }

    if not approved:
        logger.debug("[V6.Confidence] %s %s rejected: %s", engine_name, direction, reason)

    return ConfidenceResult(
        engine=engine_name,
        direction=direction,
        raw_votes=raw_votes,
        engine_weight=weight,
        confidence_score=final_score,
        min_threshold=threshold,
        approved=approved,
        reason=reason,
        regime_mult=regime_mult,
        perf_mult=perf_mult,
        vol_adj=vol_adj,
        mtf_boost=mtf_boost,
        opposition_penalty=opp_penalty,
        details=details,
    )


def normalised_confidence(result: ConfidenceResult) -> float:
    """Return confidence_score as a 0–1 fraction of the maximum possible score."""
    max_possible = (_MAX_RAW_VOTES * result.engine_weight
                    * (1.0 + float(_ss.get_val("v6_regime_trend_boost", 0.15)))
                    + float(_ss.get_val("v6_mtf_align_boost", 0.20)))
    if max_possible <= 0:
        return 0.0
    return min(result.confidence_score / max_possible, 1.0)

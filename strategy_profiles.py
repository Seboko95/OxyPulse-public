"""
OxyPulse — strategy_profiles.py
Pre-balanced strategy profiles for futures trading.
Each profile activates a compatible set of strategies tuned for one market regime.
Profiles eliminate contradicting signals that waste capital or cause opposing trades.

Profiles:
  MAX_YIELD   — volatile/trending: fast momentum strategies, highest trade frequency
  PRECISION   — ranging: confirmation-heavy, structural edge, lower frequency
  TREND_RIDER — trending: EMA + momentum + fundamental confirmation
  SAFE_GROWTH — any regime: conservative, multiple confirmations, capital protection
  AUTO        — regime-selected automatically (recommended)

Each profile includes:
  - strategies: list of strategy keys to activate
  - risk_override: optional multipliers for risk_pct, atr_sl_mult
  - min_signals: minimum agreeing strategies to enter
  - recommended_regime: which regime this profile excels in
"""
from __future__ import annotations
import logging
from dataclasses import dataclass, field

import settings_store as _s
import regime_detector as _r

logger = logging.getLogger("OxyPulse.Profiles")


@dataclass
class Profile:
    name:               str
    display:            str
    emoji:              str
    description:        str
    strategies:         list[str]
    min_signals:        int
    risk_multiplier:    float   # multiplied against base risk_pct
    recommended_regime: list[str]
    notes:              str = ""


# ── Profile definitions ────────────────────────────────────────────────────────

PROFILES: dict[str, Profile] = {

    "MAX_YIELD": Profile(
        name               = "MAX_YIELD",
        display            = "⚡ Max Yield",
        emoji              = "⚡",
        description        = "Highest frequency, momentum-driven. For volatile/trending markets.",
        strategies         = ["scalp", "momentum", "liq_hunt", "oi_momentum", "taker_flow", "breakout", "trend_follow", "ai_momentum"],
        min_signals        = 2,
        risk_multiplier    = 1.0,
        recommended_regime = [_r.TRENDING, _r.VOLATILE],
        notes              = "AI Momentum adds a 38-feature multi-horizon vote on top of fast technical strategies.",
    ),

    "PRECISION": Profile(
        name               = "PRECISION",
        display            = "🎯 Precision",
        emoji              = "🎯",
        description        = "High win-rate, confirmation-heavy. For ranging/sideways markets.",
        strategies         = ["sr_bounce", "funding_rate", "oi_momentum", "ls_contrarian", "ai_momentum"],
        min_signals        = 2,
        risk_multiplier    = 0.85,
        recommended_regime = [_r.RANGING],
        notes              = "AI Momentum mean-reversion features align well with SR bounce in ranging markets.",
    ),

    "TREND_RIDER": Profile(
        name               = "TREND_RIDER",
        display            = "📈 Trend Rider",
        emoji              = "📈",
        description        = "Catches and rides medium-term directional moves.",
        strategies         = ["momentum", "taker_flow", "oi_momentum", "funding_rate", "trend_follow", "breakout", "ai_momentum"],
        min_signals        = 2,
        risk_multiplier    = 1.0,
        recommended_regime = [_r.TRENDING],
        notes              = "AI Momentum multi-horizon normalisation confirms sustained trend has momentum across 1H/4H/24H.",
    ),

    "SAFE_GROWTH": Profile(
        name               = "SAFE_GROWTH",
        display            = "🛡 Safe Growth",
        emoji              = "🛡",
        description        = "Capital protection mode. Multiple confirmations, tightest filters.",
        strategies         = ["sr_bounce", "funding_rate", "ls_contrarian", "ai_momentum"],
        min_signals        = 3,
        risk_multiplier    = 0.6,
        recommended_regime = [_r.RANGING, _r.TRENDING, _r.VOLATILE, _r.DEAD],
        notes              = "AI Momentum acts as a veto vote — if it HOLDs, the 3-signal minimum is harder to reach.",
    ),
}

# Strategy key → human label mapping
STRATEGY_LABELS: dict[str, str] = {
    "scalp":        "Scalp",
    "momentum":     "Momentum Burst",
    "liq_hunt":     "Liq Hunt Fade",
    "sr_bounce":    "S/R Bounce",
    "funding_rate": "Funding Rate",
    "oi_momentum":  "OI Momentum",
    "taker_flow":   "Taker Flow",
    "ls_contrarian":"L/S Contrarian",
    "breakout":     "Breakout",
    "trend_follow": "Trend Follow",
    "ai_momentum":  "🤖 AI Momentum",
}


def get_profile(name: str) -> Profile | None:
    return PROFILES.get(name.upper())


def get_active_profile() -> Profile:
    """Return the currently active profile, or AUTO-select by regime."""
    key = _s.get_val("active_profile", "AUTO")
    if key == "AUTO":
        return _auto_select()
    p = PROFILES.get(key)
    return p if p else _auto_select()


def _auto_select() -> Profile:
    regime = _r.get_regime()
    # Find best matching profile
    for name, profile in PROFILES.items():
        if regime in profile.recommended_regime and name != "SAFE_GROWTH":
            return profile
    # Default to MAX_YIELD for unknown/dead — faster strategies, fires sooner
    return PROFILES["MAX_YIELD"]


def set_profile(name: str) -> bool:
    if name.upper() in PROFILES or name.upper() == "AUTO":
        _s.set_val("active_profile", name.upper())
        logger.info("Profile set to: %s", name)
        return True
    return False


def get_active_strategies() -> list[str]:
    return get_active_profile().strategies


def get_min_signals() -> int:
    profile = get_active_profile()
    base    = _s.get_val("min_signal_count", 2)
    return max(base, profile.min_signals)


def get_risk_multiplier() -> float:
    return get_active_profile().risk_multiplier


def format_profiles(current_regime: str = "") -> str:
    active  = get_active_profile()
    setting = _s.get_val("active_profile", "AUTO")
    lines   = [f"🎛 *Strategy Profiles*\n"]
    if current_regime:
        lines.append(f"Current regime: `{current_regime}` → auto-select: *{_auto_select().display}*\n")
    for name, p in PROFILES.items():
        is_active = (active.name == name)
        marker    = "✅ " if is_active else "   "
        lines.append(
            f"{marker}{p.emoji} *{p.display}*\n"
            f"     _{p.description}_\n"
            f"     Strategies: `{'` · `'.join(STRATEGY_LABELS.get(s, s) for s in p.strategies)}`\n"
            f"     Min signals: `{p.min_signals}` | Risk ×: `{p.risk_multiplier}`\n"
        )
    lines.append(f"\nSetting: `{'AUTO (regime-driven)' if setting == 'AUTO' else setting}`")
    return "\n".join(lines)

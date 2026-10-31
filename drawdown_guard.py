"""
OxyPulse V6 / V6.1 — drawdown_guard.py
Capital-adaptive anti-drawdown protection.

V6.1 upgrades over V6:
  - Thresholds are read from the capital profile's drawdown_tolerance_pct
    instead of fixed settings. MICRO tolerates more, PRO much less.
  - Level thresholds scale relative to the profile's tolerance.
  - Still coexists safely with existing daily/weekly caps in entry_filter.

Level responses:
  0 — Normal:           full sizing, normal leverage
  1 — Caution (25%+):   size ×0.8
  2 — Reduced (50%+):   size ×0.5, suggest lower leverage
  3 — Restricted (75%+):size ×0.25, only best setups
  4 — Paused (100%+):   no new entries

The percentages above are relative to tolerance_pct (not absolute %).
Example: MICRO tolerance=15% → level 4 triggers at 15% daily DD.
         PRO   tolerance=5%  → level 4 triggers at  5% daily DD.

This makes PRO mode halt at 5% daily DD while MICRO only halts at 15%.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass

import settings_store as _ss

logger = logging.getLogger("OxyPulse.V6.DrawdownGuard")

_LEVEL_LABELS = {0: "NORMAL", 1: "CAUTION", 2: "REDUCED", 3: "RESTRICTED", 4: "PAUSED"}

# Fraction of tolerance_pct at which each level triggers
_LEVEL_FRACTIONS = {
    1: 0.25,   # caution at 25% of tolerance consumed
    2: 0.50,   # reduced at 50%
    3: 0.75,   # restricted at 75%
    4: 1.00,   # paused at 100%
}


@dataclass
class DrawdownState:
    """Current drawdown protection level and enforcement parameters."""
    level: int
    label: str
    size_multiplier: float
    leverage_cap: int
    allow_new_trades: bool
    reason: str
    daily_dd_pct: float = 0.0
    weekly_dd_pct: float = 0.0
    consec_losses: int = 0
    tolerance_pct: float = 10.0


_NORMAL = DrawdownState(
    level=0, label="NORMAL", size_multiplier=1.0,
    leverage_cap=25, allow_new_trades=True,
    reason="no drawdown detected",
)


def _compute_level(
    daily_dd: float,
    weekly_dd: float,
    consec: int,
    tolerance_pct: float,
) -> int:
    """Compute drawdown level relative to the capital profile's tolerance.

    Uses fractions of tolerance_pct so MICRO and PRO respond differently
    to the same absolute drawdown.
    """
    # Convert absolute levels from settings as fallback (old V6 behaviour)
    # but override with profile-relative thresholds when a tolerance is given.
    l4 = tolerance_pct * _LEVEL_FRACTIONS[4]   # 100% of tolerance
    l3 = tolerance_pct * _LEVEL_FRACTIONS[3]   # 75%
    l2 = tolerance_pct * _LEVEL_FRACTIONS[2]   # 50%
    l1 = tolerance_pct * _LEVEL_FRACTIONS[1]   # 25%

    # Weekly DD counts at 50% weight (slower signal)
    worst = max(daily_dd, weekly_dd * 0.5)

    if worst >= l4:
        return 4
    if worst >= l3 or consec >= 8:
        return 3
    if worst >= l2 or consec >= 5:
        return 2
    if worst >= l1 or consec >= 3:
        return 1
    return 0


def _level_to_state(
    level: int,
    daily_dd: float,
    weekly_dd: float,
    consec: int,
    tolerance_pct: float,
) -> DrawdownState:
    mults   = {0: 1.0,  1: 0.8,  2: 0.5,  3: 0.25, 4: 0.0}
    levcap  = {0: 25,   1: 25,   2: 20,   3: 15,   4: 0}
    allow   = level < 4
    reasons = {
        0: "normal conditions",
        1: f"daily dd {daily_dd:.1f}% at 25% tolerance threshold (tolerance={tolerance_pct:.1f}%)",
        2: f"daily dd {daily_dd:.1f}% at 50% threshold — reduce size",
        3: f"daily dd {daily_dd:.1f}% at 75% threshold — restrict entries",
        4: f"daily dd {daily_dd:.1f}% at/above tolerance {tolerance_pct:.1f}% — PAUSED",
    }
    return DrawdownState(
        level=level,
        label=_LEVEL_LABELS[level],
        size_multiplier=mults[level],
        leverage_cap=levcap[level],
        allow_new_trades=allow,
        reason=reasons[level],
        daily_dd_pct=daily_dd,
        weekly_dd_pct=weekly_dd,
        consec_losses=consec,
        tolerance_pct=tolerance_pct,
    )


def get_drawdown_state(capital_profile=None) -> DrawdownState:
    """Read performance metrics and return DrawdownState.

    Args:
        capital_profile: CapitalProfile from capital_manager.get_profile().
                         If None, falls back to fixed settings thresholds.

    Safe: returns NORMAL if performance_tracker is unavailable or guard disabled.
    """
    if not _ss.get_val("v6_anti_drawdown_enabled", False):
        return _NORMAL

    # Determine tolerance from capital profile or settings
    if capital_profile is not None:
        tolerance_pct = float(capital_profile.drawdown_tolerance_pct)
    else:
        tolerance_pct = float(_ss.get_val("v6_drawdown_level4_pct", 10.0))

    try:
        import performance_tracker as _pt

        balance = 1.0
        daily_loss_usdt  = 0.0
        weekly_dd_usdt   = 0.0
        consec = 0

        if hasattr(_pt, "get_state"):
            state = _pt.get_state()
            daily_gain = float(getattr(state, "daily_gain_usdt", 0.0))
            daily_loss_usdt  = max(0.0, -daily_gain)   # negative gain = loss
            weekly_dd_usdt   = abs(float(getattr(state, "weekly_loss_usdt", 0.0)))
            consec           = int(getattr(state, "consec_losses", 0))

        # Try to get balance from performance_tracker or capital_profile
        if capital_profile is not None:
            balance = max(capital_profile.balance, 1.0)
        elif hasattr(_pt, "get_balance"):
            balance = max(float(_pt.get_balance()), 1.0)

        daily_dd_pct  = daily_loss_usdt  / balance * 100
        weekly_dd_pct = weekly_dd_usdt   / balance * 100

    except Exception as e:
        logger.debug("[V6.DrawdownGuard] read error: %s", e)
        return _NORMAL

    level = _compute_level(daily_dd_pct, weekly_dd_pct, consec, tolerance_pct)
    state = _level_to_state(level, daily_dd_pct, weekly_dd_pct, consec, tolerance_pct)

    if level > 0:
        logger.info(
            "[V6.DrawdownGuard] Level %d (%s): daily=%.1f%% weekly=%.1f%% consec=%d tol=%.1f%%",
            level, state.label, daily_dd_pct, weekly_dd_pct, consec, tolerance_pct,
        )

    return state

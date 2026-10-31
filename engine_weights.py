"""
OxyPulse V6 / V6.1 — engine_weights.py
Adaptive per-engine weight management.

V6.1 upgrades over V6:
  - Profit factor contributes to weight (not just win rate)
  - Drawdown contribution penalises engines that cause large losses
  - Stronger decay toward 1.0 to prevent overfitting
  - Hard bounds [v6_weight_min, v6_weight_max] unchanged

Weight update formula:
  delta = wr_delta + pf_delta - dd_penalty
  new_weight = (current + decay_toward_1) + delta
  clamped to [min, max]

Where:
  wr_delta = +0.05 if WR > 60%, −0.05 if WR < 40%, else 0
  pf_delta = profit_factor_alpha × (profit_factor − 1.0) × 0.02  (bounded ±0.03)
  dd_penalty = dd_contrib_penalty × avg_loss_per_sl × 0.01       (bounded 0..0.05)

All coefficients are configurable in settings.

Persists to data/engine_weights.json on every update.
"""
from __future__ import annotations

import json
import logging
import os
from pathlib import Path

import settings_store as _ss

logger = logging.getLogger("OxyPulse.V6.EngineWeights")

_PATH = Path("data/engine_weights.json")
_weights: dict[str, float] = {}


def _load() -> None:
    global _weights
    if _PATH.exists():
        try:
            _weights = json.loads(_PATH.read_text(encoding="utf-8"))
            return
        except Exception as e:
            logger.warning("[V6.Weights] load failed: %s — using defaults", e)
    _weights = {}


def _save() -> None:
    try:
        _PATH.parent.mkdir(parents=True, exist_ok=True)
        _PATH.write_text(json.dumps(_weights, indent=2, ensure_ascii=False), encoding="utf-8")
    except Exception as e:
        logger.warning("[V6.Weights] save failed: %s", e)


def _default_weight(engine_name: str) -> float:
    return float(_ss.get_val(f"v6_weight_{engine_name.lower()}", 1.0))


def get_weight(engine_name: str) -> float | None:
    """Return the live weight for *engine_name*, or the settings default."""
    if not _weights:
        _load()
    key = engine_name.upper()
    if key in _weights:
        return float(_weights[key])
    return _default_weight(engine_name)


def get_all_weights() -> dict[str, float]:
    if not _weights:
        _load()
    return dict(_weights)


def update_weight(
    engine_name: str,
    win_rate: float,
    num_trades: int,
    profit_factor: float = 1.0,
    avg_loss_usdt: float = 0.0,
) -> float:
    """Adjust the weight for *engine_name* using multi-factor performance.

    Args:
        engine_name:    Uppercase engine identifier.
        win_rate:       Recent win rate 0.0–1.0.
        num_trades:     Sample size.
        profit_factor:  Gross profit / gross loss (>1 = profitable).
        avg_loss_usdt:  Average loss per losing trade in USDT (for DD contribution).

    Returns:
        New weight after adaptation + decay.
    """
    if not _ss.get_val("engine_weight_adaptation_enabled", False):
        return get_weight(engine_name) or 1.0

    min_trades = int(_ss.get_val("v6_weight_update_min_trades", 5))
    if num_trades < min_trades:
        return get_weight(engine_name) or 1.0

    w_min  = float(_ss.get_val("v6_weight_min",             0.5))
    w_max  = float(_ss.get_val("v6_weight_max",             3.0))
    decay  = float(_ss.get_val("v6_weight_decay_rate",      0.02))
    pf_alpha  = float(_ss.get_val("v6_weight_profit_factor_alpha",  0.3))
    dd_pen    = float(_ss.get_val("v6_weight_dd_contrib_penalty",   0.1))

    if not _weights:
        _load()

    key     = engine_name.upper()
    current = float(_weights.get(key, _default_weight(engine_name)))

    # ── Win-rate delta ─────────────────────────────────────────────────────
    if win_rate > 0.60:
        wr_delta = 0.05
    elif win_rate < 0.40:
        wr_delta = -0.05
    else:
        wr_delta = 0.0

    # ── Profit factor delta ────────────────────────────────────────────────
    # profit_factor > 1 → profitable; < 1 → unprofitable
    pf_delta = max(-0.03, min(pf_alpha * (profit_factor - 1.0) * 0.02, 0.03))

    # ── Drawdown contribution penalty ──────────────────────────────────────
    # Penalise engines with large average losses (large SL hits)
    dd_penalty = min(dd_pen * (avg_loss_usdt / 100.0) * 0.01, 0.05)

    # ── Decay toward 1.0 ──────────────────────────────────────────────────
    decayed = current + (1.0 - current) * decay

    new_weight = max(w_min, min(decayed + wr_delta + pf_delta - dd_penalty, w_max))
    _weights[key] = round(new_weight, 4)
    _save()

    logger.info(
        "[V6.Weights] %s: %.3f → %.3f (WR=%.0f%% PF=%.2f avgLoss=$%.1f "
        "wr_δ=%+.2f pf_δ=%+.3f dd_pen=-%.3f decay=%.3f)",
        engine_name, current, new_weight,
        win_rate * 100, profit_factor, avg_loss_usdt,
        wr_delta, pf_delta, dd_penalty, decayed - current,
    )
    return new_weight


def reset_weight(engine_name: str) -> None:
    if not _weights:
        _load()
    default = _default_weight(engine_name)
    _weights[engine_name.upper()] = default
    _save()
    logger.info("[V6.Weights] %s reset to %.3f", engine_name, default)


def reset_all_weights() -> None:
    global _weights
    _weights = {}
    _save()
    logger.info("[V6.Weights] all weights reset")


def maybe_update_from_tracker(engine_name: str) -> None:
    """Read category_tracker stats and update weight if enough data.

    V6.1: also reads profit_factor and avg_loss_usdt from stats if available.
    """
    if os.getenv("PAPER_TRADING", "true").strip().lower() != "true":
        logger.info("[V6.Weights] local tracker update blocked in production; exchange-final learner required")
        return
    if not _ss.get_val("engine_weight_adaptation_enabled", False):
        return
    try:
        import category_tracker as _ct
        stats     = _ct.get_engine_stats()
        eng_stats = stats.get(engine_name.upper(), {})
        total     = int(eng_stats.get("total_trades", 0))
        wr        = float(eng_stats.get("win_rate",   0.5))

        # V6.1 extras — may not exist in all category_tracker versions
        pf        = float(eng_stats.get("profit_factor", 1.0))
        avg_loss  = float(eng_stats.get("avg_loss_usdt", 0.0))

        update_weight(engine_name, wr, total, pf, avg_loss)
    except Exception as e:
        logger.debug("[V6.Weights] maybe_update error for %s: %s", engine_name, e)


_load()

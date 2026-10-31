"""Conservative, config-driven exit families keyed by strategy lane."""
from __future__ import annotations

from typing import Any

import settings_store as _s


DEFAULTS = {
    "trend": {"be": 1.20, "tp1": 1.10, "tp2": 1.20, "profit_lock": 1.15, "trail_activation": 1.20, "trail_distance": 1.25},
    "breakout": {"be": 1.10, "tp1": 1.00, "tp2": 1.10, "profit_lock": 1.05, "trail_activation": 1.10, "trail_distance": 1.10},
    "mean_reversion": {"be": 0.90, "tp1": 0.85, "tp2": 0.90, "profit_lock": 0.90, "trail_activation": 0.90, "trail_distance": 0.90},
    "order_flow": {"be": 1.00, "tp1": 0.95, "tp2": 1.00, "profit_lock": 0.95, "trail_activation": 1.00, "trail_distance": 1.00},
    "structure": {"be": 1.05, "tp1": 1.00, "tp2": 1.05, "profit_lock": 1.00, "trail_activation": 1.05, "trail_distance": 1.05},
    "event": {"be": 1.00, "tp1": 0.90, "tp2": 0.95, "profit_lock": 0.90, "trail_activation": 0.95, "trail_distance": 0.95},
}


def family_for_engine(engine: str) -> str:
    lane = str(_s.get_engine_lane(str(engine or "").upper()) or "structure").lower()
    if lane in {"protector", "selector"}:
        return "structure"
    return lane if lane in DEFAULTS else "structure"


def profile_for_engine(engine: str) -> dict[str, Any]:
    family = family_for_engine(engine)
    configured = _s.get_val("exit_profile_multipliers", DEFAULTS) or DEFAULTS
    defaults = dict(configured.get(family, DEFAULTS[family]) if isinstance(configured, dict) else DEFAULTS[family])
    values = {
        key: float(_s.get_val(f"exit_profile_{family}_{key}_mult", default) or default)
        for key, default in defaults.items()
    }
    return {
        "family": family,
        "source": "config_conservative_low_sample",
        "sample_confidence": "low",
        **values,
    }

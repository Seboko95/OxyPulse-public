from __future__ import annotations

_ROUND_TRIP_FEE_PCT = 0.10


def _is_long(side: str) -> bool:
    return str(side or "").lower() in {"buy", "long"}


def _price_at_distance(entry: float, side: str, move_pct: float) -> float:
    if _is_long(side):
        return round(entry * (1.0 + move_pct / 100.0), 8)
    return round(entry * (1.0 - move_pct / 100.0), 8)


def _projected_profit_usdt(entry: float, target: float, size_usdt: float) -> float:
    if entry <= 0 or size_usdt <= 0:
        return 0.0
    move_pct = abs(float(target) - float(entry)) / float(entry) * 100.0
    gross = float(size_usdt) * (move_pct / 100.0)
    fee = float(size_usdt) * (_ROUND_TRIP_FEE_PCT / 100.0)
    return gross - fee


def apply_profit_targeting(
    entry: float,
    side: str,
    tp1: float,
    tp2: float,
    size_usdt: float,
    *,
    tp_distance_multiplier: float = 1.0,
) -> dict:
    """
    Non-blocking TP distance adjustment.

    FINAL PASS CHANGE (2026-04-16):
    - profit_target is no longer allowed to block entries (no "min $ profit" requirement).
    - This helper now ONLY applies the caller-provided distance multiplier (trade-intent, mode, etc).
    - It never rejects a trade and never widens targets to meet a fixed USDT profit threshold.
    """
    result = {
        "skip": False,
        "skip_reason": "",
        "tp1": tp1,
        "tp2": tp2,
        "applied_multiplier": 1.0,
        "profit_reason": "base targets",
        "projected_tp1_net": round(_projected_profit_usdt(entry, tp1, size_usdt), 4),
        "projected_tp2_net": round(_projected_profit_usdt(entry, tp2, size_usdt), 4),
    }
    if entry <= 0 or size_usdt <= 0:
        return result

    base_tp1_pct = abs(float(tp1) - float(entry)) / float(entry) * 100.0
    base_tp2_pct = abs(float(tp2) - float(entry)) / float(entry) * 100.0
    desired_tp_mult = max(float(tp_distance_multiplier or 1.0), 0.25)

    if desired_tp_mult != 1.0 and base_tp1_pct > 0 and base_tp2_pct > 0:
        result["tp1"] = _price_at_distance(entry, side, base_tp1_pct * desired_tp_mult)
        result["tp2"] = _price_at_distance(entry, side, base_tp2_pct * desired_tp_mult)
        result["applied_multiplier"] = round(desired_tp_mult, 3)
        result["projected_tp1_net"] = round(_projected_profit_usdt(entry, result["tp1"], size_usdt), 4)
        result["projected_tp2_net"] = round(_projected_profit_usdt(entry, result["tp2"], size_usdt), 4)
        result["profit_reason"] = f"TP distance multiplier {desired_tp_mult:.2f}x (non-blocking)"
    return result

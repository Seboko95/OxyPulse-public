from __future__ import annotations

from dataclasses import dataclass, field

import performance_tracker as _perf
import settings_store as _s


@dataclass(frozen=True)
class TradeIntent:
    mode: str
    size_multiplier: float
    tp_multiplier: float
    max_positions_multiplier: float
    frequency_multiplier: float
    allow_new_trades: bool
    reasons: list[str] = field(default_factory=list)

    @property
    def summary(self) -> str:
        return " | ".join(self.reasons) if self.reasons else self.mode


def _mode_payload(mode: str) -> TradeIntent:
    active = str(mode or "NORMAL").upper()
    key = active.lower()
    allow_new = active != "STOPPED"
    return TradeIntent(
        mode=active,
        size_multiplier=float(_s.get_val(f"trade_intent_{key}_size_mult", 1.0) or 1.0),
        tp_multiplier=float(_s.get_val(f"trade_intent_{key}_tp_mult", 1.0) or 1.0),
        max_positions_multiplier=float(_s.get_val(f"trade_intent_{key}_max_positions_mult", 1.0) or 1.0),
        frequency_multiplier=float(_s.get_val(f"trade_intent_{key}_freq_mult", 1.0) or 1.0),
        allow_new_trades=allow_new,
        reasons=[],
    )


def get_trade_intent(balance: float = 0.0, regime: str | None = None) -> TradeIntent:
    if not bool(_s.get_val("trade_intent_enabled", True)):
        return TradeIntent(
            mode="NORMAL",
            size_multiplier=1.0,
            tp_multiplier=1.0,
            max_positions_multiplier=1.0,
            frequency_multiplier=1.0,
            allow_new_trades=True,
            reasons=["intent disabled"],
        )

    override = str(_s.get_val("trade_intent_mode_override", "") or "").strip().upper()
    if override in {"AGGRESSIVE", "NORMAL", "DEFENSIVE", "STOPPED"}:
        payload = _mode_payload(override)
        return TradeIntent(**{**payload.__dict__, "reasons": [f"manual override {override}"]})

    metrics = _perf.get_metrics()
    min_sample = max(int(_s.get_val("trade_intent_min_sample_trades", 8) or 8), 1)
    win_rate = float(metrics.get("win_rate", 0.0) or 0.0)
    max_dd = float(metrics.get("max_dd", 0.0) or 0.0)
    total = int(metrics.get("total", 0) or 0)

    aggressive_wr = float(_s.get_val("trade_intent_aggressive_wr_pct", 50.0) or 50.0)
    defensive_wr = float(_s.get_val("trade_intent_defensive_wr_pct", 40.0) or 40.0)
    reduce_trades_wr = float(_s.get_val("trade_intent_reduce_trades_wr_pct", 35.0) or 35.0)
    stop_wr = float(_s.get_val("trade_intent_stop_wr_pct", 30.0) or 30.0)
    dd_defensive = float(_s.get_val("trade_intent_drawdown_defensive_pct", 8.0) or 8.0)

    reasons: list[str] = []
    mode = "NORMAL"

    if total >= min_sample and bool(_s.get_val("soft_kill_switch_enabled", True)):
        if win_rate < stop_wr:
            mode = "STOPPED"
            reasons.append(f"WR {win_rate:.1f}% < {stop_wr:.1f}%")
        elif win_rate < reduce_trades_wr:
            mode = "DEFENSIVE"
            reasons.append(f"WR {win_rate:.1f}% < {reduce_trades_wr:.1f}%")
        elif win_rate < defensive_wr:
            mode = "DEFENSIVE"
            reasons.append(f"WR {win_rate:.1f}% < {defensive_wr:.1f}%")
        elif win_rate > aggressive_wr:
            mode = "AGGRESSIVE"
            reasons.append(f"WR {win_rate:.1f}% > {aggressive_wr:.1f}%")

    if max_dd >= dd_defensive and mode not in {"STOPPED", "DEFENSIVE"}:
        mode = "DEFENSIVE"
        reasons.append(f"max DD {max_dd:.1f}% >= {dd_defensive:.1f}%")

    if str(regime or "").upper() == "VOLATILE" and mode == "AGGRESSIVE":
        mode = "NORMAL"
        reasons.append("volatile regime")

    payload = _mode_payload(mode)
    if not reasons:
        if total < min_sample:
            reasons.append(f"sample {total}/{min_sample}")
        else:
            reasons.append(f"WR {win_rate:.1f}%")
    return TradeIntent(**{**payload.__dict__, "reasons": reasons})

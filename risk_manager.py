"""
OxyPulse — risk_manager.py
ATR-based dynamic position sizing (the 1% risk rule).

Core formula:
  SL distance % = (ATR × sl_multiplier) / entry_price × 100
  Position size = (balance × risk_pct) / SL_distance_pct

This ensures every trade risks exactly risk_pct of account equity —
regardless of coin volatility. A volatile coin gets a smaller position,
a tight coin gets a larger one.

Max size cap: never more than 50% of balance (safety ceiling).
Size halved by performance_tracker after consecutive losses.
Auto-reduced if drawdown > threshold.
"""
from __future__ import annotations
import logging
import sqlite3
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from threading import RLock

import settings_store as _s
import performance_tracker as _perf
from indicators import atr as calc_atr

logger = logging.getLogger("OxyPulse.RiskManager")

OKX_TAKER_FEE_PCT  = 0.05
OKX_ROUND_TRIP_PCT = OKX_TAKER_FEE_PCT * 2


@dataclass
class RiskTier:
    name:            str
    min_balance:     float
    max_balance:     float
    trade_pct:       float      # legacy sizing field kept for compatibility
    max_positions:   int
    stop_loss_pct:   float      # fallback when ATR unavailable
    take_profit_pct: float      # fallback when ATR unavailable
    min_trade_usdt:  float
    daily_loss_pct:  float


_TIER_BANDS = [
    ("NANO",      2,    49),
    ("MICRO",    50,   199),
    ("SMALL",   200,   999),
    ("STANDARD",1000,  9e9),
]


def _build_tiers() -> list[RiskTier]:
    result = []
    for name, min_bal, max_bal in _TIER_BANDS:
        s = _s.get_tier(name)
        result.append(RiskTier(
            name            = name,
            min_balance     = min_bal,
            max_balance     = max_bal,
            trade_pct       = float(s.get("trade_pct",       55)),
            max_positions   = int(s.get("max_positions",    9999)),
            stop_loss_pct   = float(s.get("sl_pct",          2.5)),
            take_profit_pct = float(s.get("tp_pct",          6.10)),
            min_trade_usdt  = float(s.get("min_trade_usdt",  5.0)),
            daily_loss_pct  = float(_s.get_val("daily_loss_pct", s.get("daily_loss_pct", 40.0))),
        ))
    return result


# TTL cache for _build_tiers() — rebuilds at most once per 60 s.
# Avoids allocating 4 new RiskTier objects on every get_tier() call
# (called per-coin per-cycle = thousands of times per minute at scale).
_tiers_cache:    list[RiskTier] = []
_tiers_cache_ts: float          = 0.0
_TIERS_TTL:      float          = 60.0   # seconds


def _get_tiers() -> list[RiskTier]:
    """Return cached tier list, rebuilding only when TTL has expired."""
    global _tiers_cache, _tiers_cache_ts
    now = time.monotonic()
    if not _tiers_cache or (now - _tiers_cache_ts) > _TIERS_TTL:
        _tiers_cache    = _build_tiers()
        _tiers_cache_ts = now
    return _tiers_cache


def fee_cost_usdt(size_usdt: float, leverage: int = 1) -> float:
    # size_usdt is already the leveraged notional on exchange.
    try:
        from live_fee_schedule import round_trip_cost

        return round(round_trip_cost(size_usdt, execution_type="market"), 6)
    except Exception:
        return round(size_usdt * OKX_ROUND_TRIP_PCT / 100, 6)


def net_profit_usdt(size_usdt: float, tp_pct: float, leverage: int = 1) -> float:
    return round(size_usdt * (tp_pct / 100) - fee_cost_usdt(size_usdt, leverage), 6)


def net_loss_usdt(size_usdt: float, sl_pct: float, leverage: int = 1) -> float:
    return round(size_usdt * (sl_pct / 100) + fee_cost_usdt(size_usdt, leverage), 6)


def reward_risk_ratio(size_usdt: float, tier: RiskTier, leverage: int = 1) -> float:
    net_tp = net_profit_usdt(size_usdt, tier.take_profit_pct, leverage)
    net_sl = net_loss_usdt(size_usdt, tier.stop_loss_pct, leverage)
    return round(net_tp / net_sl, 2) if net_sl > 0 else 0.0


def breakeven_pct(leverage: int = 1) -> float:
    """Minimum TP% needed to cover round-trip fees."""
    return round(OKX_ROUND_TRIP_PCT * 1.5, 4)


def fee_aware_tp_price(entry: float, side: str, tp_price: float, leverage: int = 1, min_rr: float = 1.5) -> float:
    """Validate that TP price produces profit after fees; push out if not."""
    be_pct = breakeven_pct(leverage)
    if side in ("buy", "long"):
        min_tp = round(entry * (1 + be_pct / 100), 8)
        return max(tp_price, min_tp)
    else:
        min_tp = round(entry * (1 - be_pct / 100), 8)
        return min(tp_price, min_tp)


def total_open_risk_pct(open_trades: dict, balance: float) -> float:
    """Sum the % risk across all open positions (3-5-7 Rule)."""
    if not open_trades or balance <= 0:
        return 0.0
    total = 0.0
    for coin, trade in open_trades.items():
        size  = float(trade.get("size_usdt", trade.get("size", 0)) or 0)
        lev   = float(trade.get("leverage", 1))
        entry = float(trade.get("entry", 0))
        sl    = float(trade.get("stop_loss", 0))
        if entry <= 0 or sl <= 0 or size <= 0:
            # Fallback: estimate risk using min SL distance (conservative but not extreme).
            # Avoids treating full notional as at-risk which would lock out all new trades.
            _est_sl_pct = float(_s.get_val("min_sl_dist_pct", 1.0))
            total += (size * _est_sl_pct / 100) / balance * 100
            continue
        sl_dist_pct  = abs(entry - sl) / entry * 100
        # size is already notional (leveraged) — do NOT multiply by lev again
        pos_risk_pct = (size * sl_dist_pct / 100) / balance * 100
        total        += pos_risk_pct
    return round(total, 3)


def aggregate_unrealized_pnl_usdt(open_trades: dict | None) -> float:
    """Best-effort aggregate open PnL from registry rows and latest local prices."""
    if not open_trades:
        return 0.0
    try:
        from market_data_stream import latest_price
    except Exception:
        latest_price = None
    total = 0.0
    for coin, trade in (open_trades or {}).items():
        try:
            entry = float(trade.get("entry", 0.0) or 0.0)
            size = float(trade.get("size_usdt", trade.get("size", 0.0)) or 0.0)
            side = str(trade.get("direction", trade.get("side", "")) or "").lower()
            mark = float(trade.get("mark_price", trade.get("current_price", 0.0)) or 0.0)
            if mark <= 0 and latest_price is not None:
                mark = float(latest_price(str(coin).upper()) or 0.0)
            if entry <= 0 or mark <= 0 or size <= 0:
                continue
            if side in {"short", "sell"}:
                total += (entry - mark) / entry * size
            else:
                total += (mark - entry) / entry * size
        except Exception:
            continue
    return round(total, 6)


class RiskManager:
    def __init__(self, ledger_path: Path | str | None = None):
        # Entry/close lifecycle paths own these slots.  The SQLite ledger is
        # deliberately lazy so importing the risk manager does not create a
        # runtime artifact; the first capacity check or lifecycle operation
        # reconstructs the count from its durable OPEN rows.
        self._slot_ledger_path = Path(ledger_path) if ledger_path is not None else Path("data/risk_slots.sqlite3")
        self._slot_ledger_lock = RLock()
        self._open_positions = 0
        self._daily_loss_usdt = 0.0
        self._daily_gain_usdt = 0.0
        self._daily_realized_pnl_usdt = 0.0
        self._open_unrealized_pnl_usdt = 0.0
        self._daily_total_pnl_usdt = 0.0
        self._daily_cap_halted = False
        self._daily_cap_halt_reason = ""
        self._daily_cap_halt_ts = ""
        self._daily_cap_review_required = False
        self._last_balance_usdt = 0.0
        self._start_balance   = None

    def _tiers(self) -> list[RiskTier]:
        return _get_tiers()

    def get_tier(self, balance_usdt: float) -> RiskTier:
        tiers = self._tiers()
        for tier in reversed(tiers):
            if balance_usdt >= tier.min_balance:
                return tier
        return tiers[0]

    def calculate_atr_size(
        self,
        balance_usdt: float,
        entry_price:  float,
        atr_val:      float | None,
        leverage:     int = 1,
    ) -> dict:
        """
        ATR-based position sizing (the 1% risk rule).
        Returns size_usdt, sl_price, tp1_price, tp2_price, sl_pct.
        """
        tier      = self.get_tier(balance_usdt)
        risk_pct  = _s.get_val("risk_pct", 0.75)
        risk_mult = 1.0

        # Apply profile risk multiplier
        try:
            from strategy_profiles import get_risk_multiplier
            risk_mult = get_risk_multiplier()
        except Exception:
            pass

        # Auto-reduce if drawdown threshold hit
        if _perf.is_drawdown_auto_reduce(balance_usdt):
            reduce_factor = max(min(float(_s.get_val("risk_auto_reduce_factor", 0.5) or 0.5), 1.0), 0.1)
            risk_mult *= reduce_factor
            logger.info("Risk: drawdown auto-reduce active — size reduced to x%.2f.", reduce_factor)

        # Win streak boost
        win_streak_mult = _perf.get_win_streak_multiplier()
        if win_streak_mult > 1.0:
            risk_mult = min(risk_mult * win_streak_mult, _s.get_val("risk_pct_max", 1.0) / max(risk_pct, 0.01))
            logger.debug("Risk: win streak boost × %.2f (streak=%d)", win_streak_mult, _perf.get_win_streak())

        # Consecutive loss reduction
        size_mult = _perf.get_size_multiplier()

        effective_risk = risk_pct * risk_mult
        risk_usdt      = balance_usdt * (effective_risk / 100)

        sl_mult  = _s.get_val("atr_sl_multiplier", 2.3)
        tp1_mult = _s.get_val("atr_tp1_multiplier", 5.0)
        tp2_mult = _s.get_val("atr_tp2_multiplier", 10.0)

        if atr_val and atr_val > 0 and entry_price > 0:
            sl_dist_price = atr_val * sl_mult
            sl_dist_pct   = sl_dist_price / entry_price * 100
        else:
            sl_dist_pct   = tier.stop_loss_pct
            sl_dist_price = entry_price * sl_dist_pct / 100

        # Enforce SL floor and ceiling (match calculate_stop_take logic)
        min_sl_pct = float(_s.get_val("min_sl_dist_pct", 1.0))
        max_sl_pct = float(_s.get_val("max_sl_dist_pct", 3.0))
        if sl_dist_pct < min_sl_pct:
            sl_dist_pct   = min_sl_pct
            sl_dist_price = entry_price * sl_dist_pct / 100
        elif sl_dist_pct > max_sl_pct:
            sl_dist_pct   = max_sl_pct
            sl_dist_price = entry_price * sl_dist_pct / 100

        # Core sizing formula
        if sl_dist_pct > 0:
            size_usdt = round((risk_usdt / sl_dist_pct) * 100, 2)
        else:
            size_usdt = round(balance_usdt * 0.05, 2)

        # Apply consecutive loss reduction
        size_usdt = round(size_usdt * size_mult, 2)

        # Hard caps: min from settings ($100 notional = $10 margin at 10x),
        # max 50% of balance — raised from 40% to accommodate larger floor on small accounts.
        min_size  = max(_s.get_effective_min_order_usdt(balance_usdt), tier.min_trade_usdt, 1.0)
        max_size  = max(round(balance_usdt * 0.50, 2), min_size)   # floor=min_size so cap ≥ floor
        size_usdt = max(min_size, min(size_usdt, max_size))

        return {
            "size_usdt":          size_usdt,
            "sl_dist_pct":        round(sl_dist_pct, 4),
            "sl_dist_price":      round(sl_dist_price, 8),
            "risk_usdt":          round(risk_usdt, 4),
            "effective_risk_pct": round(effective_risk, 4),
            "tier":               tier,
        }

    def validate_trade(self, coin: str, side: str, balance_usdt: float, open_trades: dict = None) -> dict:
        try:
            self._refresh_open_slots()
        except Exception as exc:
            return {"valid": False, "reason": f"Risk slot ledger unavailable: {type(exc).__name__}", "tier": self.get_tier(balance_usdt)}
        self._maybe_auto_reset_daily()
        tier = self.get_tier(balance_usdt)
        self._last_balance_usdt = float(balance_usdt or 0.0)
        if self._start_balance is None:
            self._start_balance = balance_usdt
        self._sync_daily_from_performance(open_trades)

        # 3-5-7 Rule: total open risk cap
        if _s.get_val("rule_357_enabled", True) and open_trades:
            open_risk = total_open_risk_pct(open_trades, balance_usdt)
            max_open  = _s.get_val("rule_357_max_open_risk_pct", 5.0)
            if open_risk >= max_open:
                return {"valid": False, "reason": f"3-5-7 Rule: open risk {open_risk:.1f}% >= {max_open}% cap.", "tier": tier}

        # Daily cap check
        if _s.is_daily_cap_enabled():
            if self._daily_cap_halted:
                return {"valid": False, "reason": self._daily_cap_halt_reason or "Daily loss cap halt active.", "tier": tier}
            hit, pct = self._check_daily_cap(balance_usdt, tier)
            if hit:
                self._trip_daily_cap_halt(balance_usdt, pct, source="admission_check")
                return {"valid": False, "reason": f"Daily loss cap hit ({pct:.1f}%).", "tier": tier}

        # Max positions check
        if self._open_positions >= tier.max_positions:
            return {"valid": False, "reason": f"Max {tier.max_positions} positions.", "tier": tier}

        raw  = balance_usdt * (tier.trade_pct / 100)
        size = max(tier.min_trade_usdt, max(1.0, round(raw, 2)))
        size = round(size * _perf.get_size_multiplier(), 2)

        return {"valid": True, "reason": "OK", "size_usdt": size, "tier": tier}

    def _check_daily_cap(self, balance_usdt: float, tier: RiskTier) -> tuple[bool, float]:
        if balance_usdt <= 0:
            return False, 0.0
        loss_pct = (self._daily_loss_usdt / balance_usdt) * 100
        return loss_pct >= tier.daily_loss_pct, round(loss_pct, 2)

    def _sync_daily_from_performance(self, open_trades: dict | None = None) -> None:
        try:
            realized = float(_perf.get_today_realized_net_pnl())
        except Exception:
            return
        open_unrealized = aggregate_unrealized_pnl_usdt(open_trades)
        total_pnl = realized + open_unrealized
        self._daily_realized_pnl_usdt = realized
        self._open_unrealized_pnl_usdt = open_unrealized
        self._daily_total_pnl_usdt = total_pnl
        self._daily_loss_usdt = max(-total_pnl, 0.0)
        self._daily_gain_usdt = max(total_pnl, 0.0)

    def _trip_daily_cap_halt(self, balance_usdt: float, pct: float, source: str) -> None:
        if self._daily_cap_halted:
            return
        self._daily_cap_halted = True
        self._daily_cap_halt_ts = datetime.now(timezone.utc).isoformat()
        self._daily_cap_review_required = True
        tier = self.get_tier(balance_usdt)
        self._daily_cap_halt_reason = (
            f"Daily loss cap hit ({pct:.1f}% >= {tier.daily_loss_pct:.1f}%) via {source}; "
            "new entries halted and open positions require operator review."
        )
        logger.critical(self._daily_cap_halt_reason)

    def _evaluate_daily_cap_halt(self, balance_usdt: float, source: str) -> tuple[bool, float]:
        if not _s.is_daily_cap_enabled():
            return False, 0.0
        if balance_usdt <= 0:
            return False, 0.0
        tier = self.get_tier(balance_usdt)
        hit, pct = self._check_daily_cap(balance_usdt, tier)
        if hit:
            self._trip_daily_cap_halt(balance_usdt, pct, source=source)
        return hit, pct

    def _daily_cap_reference_balance(self, balance_usdt: float | None = None) -> float:
        ref = float(balance_usdt or self._last_balance_usdt or self._start_balance or 0.0)
        if ref > 0:
            return ref
        try:
            return float(getattr(_perf._state, "equity_peak", 0.0) or 0.0)
        except Exception:
            return 0.0

    def record_realized_pnl(self, pnl_usdt: float, *, balance_usdt: float | None = None, source: str = "realized_close") -> None:
        self._maybe_auto_reset_daily()
        pnl = float(pnl_usdt or 0.0)
        self._daily_realized_pnl_usdt += pnl
        self._daily_total_pnl_usdt = self._daily_realized_pnl_usdt + self._open_unrealized_pnl_usdt
        self._daily_loss_usdt = max(-self._daily_total_pnl_usdt, 0.0)
        self._daily_gain_usdt = max(self._daily_total_pnl_usdt, 0.0)
        ref_balance = self._daily_cap_reference_balance(balance_usdt)
        if ref_balance > 0:
            self._evaluate_daily_cap_halt(ref_balance, source=source)

    def daily_cap_status(self, balance_usdt: float | None = None, open_trades: dict | None = None) -> dict:
        self._maybe_auto_reset_daily()
        self._sync_daily_from_performance(open_trades)
        ref_balance = self._daily_cap_reference_balance(balance_usdt)
        if ref_balance > 0:
            self._evaluate_daily_cap_halt(ref_balance, source="status_check")
        pct = round((self._daily_loss_usdt / ref_balance) * 100, 2) if ref_balance > 0 else 0.0
        return {
            "halted": bool(self._daily_cap_halted),
            "loss_usdt": round(float(self._daily_loss_usdt or 0.0), 4),
            "realized_pnl_usdt": round(float(self._daily_realized_pnl_usdt or 0.0), 4),
            "open_unrealized_pnl_usdt": round(float(self._open_unrealized_pnl_usdt or 0.0), 4),
            "total_pnl_usdt": round(float(self._daily_total_pnl_usdt or 0.0), 4),
            "loss_pct": pct,
            "reason": self._daily_cap_halt_reason,
            "halted_at": self._daily_cap_halt_ts,
            "open_positions_review_required": bool(self._daily_cap_review_required),
        }

    def calculate_stop_take(
        self, entry: float, side: str, tier: RiskTier,
        atr_val: float | None = None,
        leverage: int = 1,
    ) -> tuple[float, float]:
        """Returns (sl_price, tp_price). Uses ATR × multipliers for breathing room.
        TP is fee-aware. Minimum SL floor prevents SL sitting right at entry.
        """
        sl_mult    = _s.get_val("atr_sl_multiplier", 2.3)
        tp1_mult   = _s.get_val("atr_tp1_multiplier", 5.0)
        min_sl_pct = float(_s.get_val("min_sl_dist_pct", 1.0))  # floor: 1% from entry

        if atr_val and atr_val > 0:
            sl_dist = atr_val * sl_mult
            tp_dist = atr_val * tp1_mult
        else:
            sl_dist = entry * tier.stop_loss_pct   / 100
            tp_dist = entry * tier.take_profit_pct / 100

        # Enforce minimum SL distance floor
        min_sl_dist = entry * min_sl_pct / 100
        if sl_dist < min_sl_dist:
            sl_dist = min_sl_dist

        # Enforce maximum SL distance ceiling — prevents liquidation at high leverage.
        # With the current 10x baseline, the 3.0% cap keeps an added safety buffer.
        max_sl_pct  = float(_s.get_val("max_sl_dist_pct", 3.0))
        max_sl_dist = entry * max_sl_pct / 100
        if sl_dist > max_sl_dist:
            sl_dist = max_sl_dist

        # Enforce minimum R:R ratio (default 1.5:1 — TP must be at least 1.5× the SL distance)
        min_rr = float(_s.get_val("min_rr_ratio", 3.0))
        if tp_dist < sl_dist * min_rr:
            tp_dist = sl_dist * min_rr

        if side in ("buy", "long"):
            raw_tp = round(entry + tp_dist, 8)
            tp     = fee_aware_tp_price(entry, "buy", raw_tp, leverage)
            return round(entry - sl_dist, 8), tp
        else:
            raw_tp = round(entry - tp_dist, 8)
            tp     = fee_aware_tp_price(entry, "sell", raw_tp, leverage)
            return round(entry + sl_dist, 8), tp

    def calculate_trade_size(self, coin: str, balance_usdt: float) -> dict:
        return self.validate_trade(coin, "buy", balance_usdt)

    def can_open_position(self, balance_usdt: float) -> tuple[bool, str]:
        return True, "OK"

    def is_daily_limit_hit(self, balance_usdt: float) -> tuple[bool, float]:
        if not _s.is_daily_cap_enabled():
            return False, 0.0
        self._maybe_auto_reset_daily()
        self._last_balance_usdt = float(balance_usdt or 0.0)
        self._sync_daily_from_performance()
        tier = self.get_tier(balance_usdt)
        hit, pct = self._check_daily_cap(balance_usdt, tier)
        if hit:
            self._trip_daily_cap_halt(balance_usdt, pct, source="status_check")
        return hit, pct

    def _maybe_auto_reset_daily(self) -> None:
        """Reset daily counters at midnight UTC."""
        today = datetime.now(timezone.utc).date()
        if not hasattr(self, "_last_reset_date") or self._last_reset_date != today:
            if hasattr(self, "_last_reset_date") and self._last_reset_date is not None:
                logger.info("Daily loss counter auto-reset (new day).")
            self.reset_daily()
            self._last_reset_date = today

    def apply_slot_operation(self, operation_id: str, action: str) -> bool:
        """Atomically apply one deterministic lifecycle slot operation.

        Both the operation identity and the slot's desired state commit in a
        single SQLite transaction.  A fresh manager therefore derives its
        capacity from durable ``OPEN`` rows instead of replaying a process
        local increment/decrement.
        """
        operation_id = str(operation_id or "").strip()
        action = str(action or "").strip().lower()
        if not operation_id:
            raise ValueError("risk slot operation_id is required")
        if action not in {"open", "close"}:
            raise ValueError(f"invalid risk slot action: {action}")
        slot_id = self._slot_id_from_operation(operation_id, action)
        with self._slot_ledger_lock:
            connection = self._slot_connection()
            try:
                connection.execute("BEGIN IMMEDIATE")
                existing_operation = connection.execute(
                    "SELECT operation_id FROM risk_slot_operations WHERE operation_id = ?",
                    (operation_id,),
                ).fetchone()
                applied = False
                if existing_operation is None:
                    current = connection.execute(
                        "SELECT state FROM risk_slots WHERE slot_id = ?", (slot_id,)
                    ).fetchone()
                    current_state = str(current[0]) if current else ""
                    if action == "open":
                        if not current:
                            connection.execute(
                                "INSERT INTO risk_slots (slot_id, intent_id, state, updated_at) VALUES (?, ?, 'OPEN', ?)",
                                (slot_id, slot_id.removeprefix("risk-slot:"), time.time()),
                            )
                            result = "APPLIED"
                            applied = True
                        elif current_state == "OPEN":
                            result = "IDEMPOTENT"
                        else:
                            # A close tombstone is causal authority for this
                            # immutable execution intent.  A late recovery open
                            # may be audited, but it must never resurrect risk.
                            result = "SUPERSEDED"
                    else:
                        if not current:
                            # An authoritative close may arrive before a
                            # previously-persisted lifecycle open.  Preserve a
                            # durable tombstone so that stale open is harmless.
                            connection.execute(
                                "INSERT INTO risk_slots (slot_id, intent_id, state, updated_at) VALUES (?, ?, 'CLOSED', ?)",
                                (slot_id, slot_id.removeprefix("risk-slot:"), time.time()),
                            )
                            result = "APPLIED"
                            applied = True
                        elif current_state == "OPEN":
                            connection.execute(
                                "UPDATE risk_slots SET state = 'CLOSED', updated_at = ? WHERE slot_id = ?",
                                (time.time(), slot_id),
                            )
                            result = "APPLIED"
                            applied = True
                        else:
                            result = "IDEMPOTENT"
                    connection.execute(
                        "INSERT INTO risk_slot_operations "
                        "(operation_id, slot_id, action, result, applied_at) VALUES (?, ?, ?, ?, ?)",
                        (operation_id, slot_id, action.upper(), result, time.time()),
                    )
                self._open_positions = int(connection.execute(
                    "SELECT COUNT(*) FROM risk_slots WHERE state = 'OPEN'"
                ).fetchone()[0])
                connection.commit()
                return applied
            except Exception:
                connection.rollback()
                raise
            finally:
                connection.close()

    @staticmethod
    def _slot_id_from_operation(operation_id: str, action: str) -> str:
        expected = f"risk-{action}:"
        if not operation_id.startswith(expected) or not operation_id[len(expected):]:
            raise ValueError("risk slot operation identity must be deterministic by intent")
        return f"risk-slot:{operation_id[len(expected):]}"

    def _slot_connection(self) -> sqlite3.Connection:
        self._slot_ledger_path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(str(self._slot_ledger_path), timeout=5.0, isolation_level=None)
        connection.execute("PRAGMA busy_timeout = 5000")
        connection.execute(
            "CREATE TABLE IF NOT EXISTS risk_slots ("
            "slot_id TEXT PRIMARY KEY, intent_id TEXT NOT NULL, "
            "state TEXT NOT NULL CHECK(state IN ('OPEN', 'CLOSED')), updated_at REAL NOT NULL)"
        )
        connection.execute(
            "CREATE TABLE IF NOT EXISTS risk_slot_operations ("
            "operation_id TEXT PRIMARY KEY, slot_id TEXT NOT NULL, "
            "action TEXT NOT NULL CHECK(action IN ('OPEN', 'CLOSE')), "
            "result TEXT NOT NULL DEFAULT 'APPLIED' CHECK(result IN ('APPLIED', 'IDEMPOTENT', 'SUPERSEDED')), "
            "applied_at REAL NOT NULL)"
        )
        columns = {str(row[1]) for row in connection.execute("PRAGMA table_info(risk_slot_operations)")}
        if "result" not in columns:
            connection.execute(
                "ALTER TABLE risk_slot_operations ADD COLUMN result TEXT NOT NULL DEFAULT 'APPLIED'"
            )
        return connection

    def _refresh_open_slots(self) -> int:
        with self._slot_ledger_lock:
            connection = self._slot_connection()
            try:
                self._open_positions = int(connection.execute(
                    "SELECT COUNT(*) FROM risk_slots WHERE state = 'OPEN'"
                ).fetchone()[0])
                return self._open_positions
            finally:
                connection.close()

    def register_open(self):  self._open_positions += 1
    def register_close(self): self._open_positions = max(0, self._open_positions - 1)
    def record_loss(self, u): self.record_realized_pnl(-abs(float(u or 0.0)), source="record_loss")
    def record_gain(self, u): self.record_realized_pnl(abs(float(u or 0.0)), source="record_gain")

    def reset_daily(self):
        self._daily_loss_usdt = 0.0
        self._daily_gain_usdt = 0.0
        self._daily_realized_pnl_usdt = 0.0
        self._open_unrealized_pnl_usdt = 0.0
        self._daily_total_pnl_usdt = 0.0
        self._daily_cap_halted = False
        self._daily_cap_halt_reason = ""
        self._daily_cap_halt_ts = ""
        self._daily_cap_review_required = False
        self._start_balance   = None

    def get_vol_adjusted_leverage(self, atr_val: float | None, price: float, base_leverage: int) -> int:
        """Reduces leverage when volatility is high, allows slight increase when calm."""
        if not _s.get_val("vol_leverage_enabled", False) or not atr_val or price <= 0:
            return base_leverage

        atr_pct     = atr_val / price * 100
        high_thresh = _s.get_val("vol_leverage_high_atr", 2.0)
        low_thresh  = _s.get_val("vol_leverage_low_atr", 0.4)
        max_lev     = _s.get_val("leverage_max", 10)

        if atr_pct >= high_thresh:
            adjusted = max(1, base_leverage // 2)
            logger.debug("VolLev: ATR=%.2f%% HIGH → leverage %d→%d", atr_pct, base_leverage, adjusted)
            return adjusted
        elif atr_pct <= low_thresh:
            adjusted = min(max_lev, base_leverage + 2)
            logger.debug("VolLev: ATR=%.2f%% LOW → leverage %d→%d", atr_pct, base_leverage, adjusted)
            return adjusted
        return base_leverage

    def summary(self, balance_usdt: float) -> str:
        try:
            import market_mode as mm
            lev = mm.get_leverage()
        except Exception:
            lev = _s.get_val("leverage_default", 5)
        tier     = self.get_tier(balance_usdt)
        risk_pct = _s.get_val("risk_pct", 0.75)
        dd_pct   = _perf.get_drawdown_pct(balance_usdt)
        cap_on   = _s.is_daily_cap_enabled()
        hit, pct = self.is_daily_limit_hit(balance_usdt)
        sl_mult  = _s.get_val("atr_sl_multiplier",  2.0)
        tp1_mult = _s.get_val("atr_tp1_multiplier",  5.0)
        tp2_mult = _s.get_val("atr_tp2_multiplier", 10.0)
        partial  = "ON" if _s.get_val("partial_exit_enabled") else "OFF"
        daily_str = f"`{tier.daily_loss_pct}%` cap" if cap_on else "disabled"
        halved_str = "YES" if _perf.is_size_halved() else "NO"
        return (
            f"*Risk: `{tier.name}` tier*\n"
            f"  Balance:    `${balance_usdt:,.2f}`\n"
            f"  Risk/trade: `{risk_pct}%`\n"
            f"  Leverage:   `{lev}x`\n"
            f"  SL: `{sl_mult}x ATR` | TP1: `{tp1_mult}x ATR` | TP2: `{tp2_mult}x ATR`\n"
            f"  Partial exits: `{partial}`\n"
            f"  Daily cap: {daily_str}\n"
            f"  Drawdown:  `{dd_pct}%` from peak\n"
            f"  Size halved: `{halved_str}`\n"
            f"  Open trades: `{self._open_positions}`"
        )


risk = RiskManager()

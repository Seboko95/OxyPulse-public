"""
OxyPulse — performance_tracker.py
Tracks live trade performance: Sharpe, Calmar, drawdown, consecutive losses.
Auto-reduces aggression when drawdown threshold is hit.
Persists to data/performance.json.
"""
from __future__ import annotations
import json
import logging
import math
import os
import tempfile
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import settings_store as _s

logger = logging.getLogger("OxyPulse.Performance")
_PATH  = Path("data/performance.json")
_DUPLICATE_CLOSE_REASON_GROUPS = {
    "sl_exchange": "exchange_close",
    "tp_exchange": "exchange_close",
    "sync_confirmed_close": "exchange_close",
    "breakeven_stop": "exchange_close",
    "trailing_stop": "exchange_close",
    "trail": "exchange_close",
}


def _trade_outcome(trade) -> str:
    reason = str(getattr(trade, "exit_reason", "") or "").lower()
    gross = float(getattr(trade, "pnl_usdt", 0.0) or 0.0)
    if reason == "slot_rotation" and abs(gross) < 1e-9:
        return "neutral"
    net = float(getattr(trade, "net_pnl_usdt", gross) or 0.0)
    return "win" if net > 0 else "loss"


@dataclass
class TradeRecord:
    coin:       str
    direction:  str
    entry:      float
    exit:       float
    size_usdt:  float
    pnl_usdt:   float
    pnl_pct:    float
    opened_at:  str
    closed_at:  str
    exit_reason:str   # "tp1" | "tp2" | "sl" | "manual" | "trail"
    fee_usdt:   float = 0.0
    net_pnl_usdt: float = 0.0
    event_id:   str = ""


@dataclass
class PerformanceState:
    trades:             list[TradeRecord] = field(default_factory=list)
    equity_peak:        float = 0.0
    weekly_loss_usdt:   float = 0.0
    # Net weekly PnL (can recover with wins). Used for weekly cap to avoid "permanent block until reset"
    # behavior from gross-loss accumulation.
    weekly_pnl_usdt:    float = 0.0
    weekly_reset_ts:    str   = ""
    # Timestamp for the last operator-initiated session reset (not the weekly auto-reset).
    session_reset_ts:   str   = ""
    consec_losses:      int   = 0
    size_halved:        bool  = False
    blackswan_halted:   bool  = False
    total_fees_usdt:    float = 0.0
    consec_wins:        int   = 0
    daily_gain_usdt:    float = 0.0
    applied_event_ids:  list[str] = field(default_factory=list)


_state = PerformanceState()


# ── Persistence ───────────────────────────────────────────────────────────────

def _save():
    try:
        _PATH.parent.mkdir(parents=True, exist_ok=True)
        data = asdict(_state)
        payload = json.dumps(data, indent=2, ensure_ascii=False)
        fd, temp_name = tempfile.mkstemp(prefix=f".{_PATH.name}.", suffix=".tmp", dir=str(_PATH.parent))
        try:
            with os.fdopen(fd, "w", encoding="utf-8", newline="") as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temp_name, _PATH)
        except Exception:
            try:
                os.unlink(temp_name)
            except OSError:
                pass
            raise
    except Exception as e:
        logger.warning("Performance save failed: %s", e)


def _load():
    global _state
    try:
        if _PATH.exists():
            data = json.loads(_PATH.read_text(encoding="utf-8"))
            trades = [TradeRecord(**t) for t in data.get("trades", [])]
            applied_event_ids = [
                str(value) for value in list(data.get("applied_event_ids", []) or [])
                if str(value or "").strip()
            ]
            for trade in trades:
                if trade.event_id and trade.event_id not in applied_event_ids:
                    applied_event_ids.append(trade.event_id)
            _state = PerformanceState(
                trades           = trades,
                equity_peak      = float(data.get("equity_peak", 0)),
                weekly_loss_usdt = float(data.get("weekly_loss_usdt", 0)),
                weekly_pnl_usdt  = float(data.get("weekly_pnl_usdt", -float(data.get("weekly_loss_usdt", 0) or 0.0))),
                weekly_reset_ts  = str(data.get("weekly_reset_ts", "")),
                session_reset_ts = str(data.get("session_reset_ts", "")),
                consec_losses    = int(data.get("consec_losses", 0)),
                size_halved      = bool(data.get("size_halved", False)),
                blackswan_halted = bool(data.get("blackswan_halted", False)),
                total_fees_usdt  = float(data.get("total_fees_usdt", 0)),
                consec_wins      = int(data.get("consec_wins", 0)),
                daily_gain_usdt  = float(data.get("daily_gain_usdt", 0)),
                applied_event_ids = applied_event_ids,
            )
    except Exception as e:
        logger.warning("Performance load failed: %s", e)


# ── Recording ─────────────────────────────────────────────────────────────────

def _parse_closed_at(value: str) -> datetime | None:
    try:
        return datetime.fromisoformat(str(value or ""))
    except Exception:
        return None


def _reason_group(reason: str) -> str:
    reason_s = str(reason or "").strip().lower()
    return _DUPLICATE_CLOSE_REASON_GROUPS.get(reason_s, reason_s)


def _is_duplicate_close(rec: TradeRecord, window_sec: float = 180.0) -> bool:
    if not _state.trades:
        return False
    group = _reason_group(rec.exit_reason)
    if not group or group.startswith("milestone_"):
        return False
    closed_at = _parse_closed_at(rec.closed_at)
    if closed_at is None:
        return False
    coin = str(rec.coin or "").upper()
    direction = str(rec.direction or "").lower()
    for prev in reversed(_state.trades[-12:]):
        prev_closed = _parse_closed_at(prev.closed_at)
        if prev_closed is None:
            continue
        if abs((closed_at - prev_closed).total_seconds()) > window_sec:
            continue
        if str(prev.coin or "").upper() != coin:
            continue
        if str(prev.direction or "").lower() != direction:
            continue
        if _reason_group(prev.exit_reason) != group:
            continue
        same_entry = abs(float(prev.entry or 0.0) - float(rec.entry or 0.0)) <= max(abs(float(rec.entry or 0.0)) * 1e-6, 1e-8)
        same_exit = abs(float(prev.exit or 0.0) - float(rec.exit or 0.0)) <= max(abs(float(rec.exit or 0.0)) * 1e-6, 1e-8)
        same_size = abs(float(prev.size_usdt or 0.0) - float(rec.size_usdt or 0.0)) <= max(abs(float(rec.size_usdt or 0.0)) * 0.01, 0.01)
        if same_entry and same_exit and same_size:
            return True
    return False

def record_trade(
    coin: str, direction: str, entry: float, exit_price: float,
    size_usdt: float, pnl_usdt: float, exit_reason: str = "tp1",
    fee_usdt: float = 0.0,
    opened_at: str = "",
    closed_at: str = "",
    event_id: str = "",
) -> None:
    if entry <= 1e-9 or abs(pnl_usdt) > 10_000:
        import logging; logging.getLogger("OxyPulse").warning("record_trade: rejected phantom trade %s entry=%s pnl=%s", coin, entry, pnl_usdt)
        return
    fee_usdt = round(float(fee_usdt or 0.0), 6)
    net_pnl_usdt = round(float(pnl_usdt or 0.0) - fee_usdt, 4)
    pnl_pct = round((pnl_usdt / size_usdt) * 100, 2) if size_usdt else 0.0
    closed_ts = str(closed_at or "").strip() or datetime.now(timezone.utc).isoformat()
    opened_ts = str(opened_at or "").strip() or closed_ts
    rec = TradeRecord(
        coin        = coin,
        direction   = direction,
        entry       = entry,
        exit        = exit_price,
        size_usdt   = size_usdt,
        pnl_usdt    = round(pnl_usdt, 4),
        fee_usdt    = fee_usdt,
        net_pnl_usdt= net_pnl_usdt,
        pnl_pct     = pnl_pct,
        opened_at   = opened_ts,
        closed_at   = closed_ts,
        exit_reason = exit_reason,
        event_id    = str(event_id or "").strip(),
    )
    if rec.event_id and rec.event_id in set(_state.applied_event_ids):
        logger.info("record_trade: suppressed already-applied economic event %s", rec.event_id)
        return
    if rec.event_id and any(str(prev.event_id or "") == rec.event_id for prev in _state.trades):
        if rec.event_id not in _state.applied_event_ids:
            _state.applied_event_ids.append(rec.event_id)
            _save()
        logger.info("record_trade: suppressed duplicate economic event %s", rec.event_id)
        return
    if _is_duplicate_close(rec):
        logger.warning(
            "record_trade: suppressed duplicate close %s direction=%s reason=%s entry=%.8f exit=%.8f size=%.4f",
            coin,
            direction,
            exit_reason,
            entry,
            exit_price,
            size_usdt,
        )
        return
    _state.trades.append(rec)
    if rec.event_id:
        _state.applied_event_ids.append(rec.event_id)
    _state.total_fees_usdt += fee_usdt
    _state.weekly_pnl_usdt += float(net_pnl_usdt or 0.0)

    outcome = _trade_outcome(rec)
    if outcome == "loss":
        _state.weekly_loss_usdt += abs(net_pnl_usdt)
        _state.consec_losses    += 1
        _state.consec_wins       = 0
    elif outcome == "win":
        _state.consec_losses  = 0
        _state.consec_wins   += 1
        _state.daily_gain_usdt += net_pnl_usdt
        if _state.size_halved:
            _state.size_halved = False
            logger.info("Performance: consecutive loss recovery — size restored.")

    _check_consec_loss_reduction()
    _save()
    _notify_risk_realized_pnl(net_pnl_usdt)


def _trade_net_pnl(trade: TradeRecord) -> float:
    return float(getattr(trade, "net_pnl_usdt", trade.pnl_usdt) or 0.0)


def _is_today_trade(trade: TradeRecord, now: datetime | None = None) -> bool:
    now = now or datetime.now(timezone.utc)
    try:
        closed = datetime.fromisoformat(str(trade.closed_at or ""))
    except Exception:
        return False
    return closed.date() == now.date()


def get_today_realized_net_pnl() -> float:
    now = datetime.now(timezone.utc)
    return round(sum(_trade_net_pnl(t) for t in _state.trades if _is_today_trade(t, now)), 4)


def get_today_realized_loss_usdt() -> float:
    return round(max(-get_today_realized_net_pnl(), 0.0), 4)


def _notify_risk_realized_pnl(net_pnl_usdt: float) -> None:
    if os.getenv("PAPER_TRADING", "true").strip().lower() != "true":
        logger.info("Live PnL remains provisional; risk manager awaits exchange reconciliation.")
        return
    try:
        from risk_manager import risk
        risk.record_realized_pnl(float(net_pnl_usdt or 0.0), source="performance_tracker")
    except Exception as exc:
        logger.error("Performance: failed to notify risk manager of realized PnL: %s", exc)


def update_equity_peak(balance: float) -> None:
    if balance > _state.equity_peak:
        _state.equity_peak = balance
        _save()


def set_blackswan(halted: bool) -> None:
    _state.blackswan_halted = halted
    _save()


# ── Checks ────────────────────────────────────────────────────────────────────

def _check_consec_loss_reduction():
    limit = int(_s.get_val("consec_loss_limit", 3))
    if _state.consec_losses >= limit and not _state.size_halved:
        _state.size_halved = True
        factor = max(min(float(_s.get_val("risk_auto_reduce_factor", 0.5) or 0.5), 1.0), 0.1)
        logger.warning(
            "Performance: %d consecutive losses — position size reduced to x%.2f until recovery.",
            _state.consec_losses,
            factor,
        )
        _save()


def is_blackswan_halted() -> bool:
    return _state.blackswan_halted


def is_size_halved() -> bool:
    return _state.size_halved


def get_size_multiplier() -> float:
    """Returns the active loss-recovery size multiplier, or 1.0 when inactive."""
    if not _state.size_halved:
        return 1.0
    return max(min(float(_s.get_val("risk_auto_reduce_factor", 0.5) or 0.5), 1.0), 0.1)


def is_weekly_cap_hit(balance: float) -> tuple[bool, float]:
    """Return (hit, pct) using net weekly PnL (losses only).

    Note: weekly cap admission is now controlled by weekly_cap_mode (soft/strict/adaptive)
    in category_engine; this helper remains for UI/diagnostics.
    """
    if not _s.get_val("weekly_cap_enabled", False):
        return False, 0.0
    # Auto-reset every Monday 00:00 UTC
    _maybe_auto_reset_weekly()
    # Backward compat: threshold key defaults to weekly_loss_pct.
    base = float(_s.get_val("weekly_loss_pct", 8.0) or 8.0)
    limit = float(_s.get_val("weekly_cap_threshold_pct", base) or base)
    weekly_net_loss = max(-float(_state.weekly_pnl_usdt or 0.0), 0.0)
    pct = (weekly_net_loss / balance) * 100 if balance else 0.0
    return pct >= limit, round(pct, 2)


def _maybe_auto_reset_weekly() -> None:
    """Reset weekly loss counter if a new ISO week has started since last reset."""
    now = datetime.now(timezone.utc)
    if _state.weekly_reset_ts:
        try:
            last = datetime.fromisoformat(_state.weekly_reset_ts)
            if last.isocalendar()[1] == now.isocalendar()[1] and last.year == now.year:
                return  # same week — no reset needed
        except (ValueError, TypeError):
            pass
    reset_weekly()
    logger.info("Weekly loss counter auto-reset (new week).")


def is_drawdown_auto_reduce(balance: float) -> bool:
    if not _state.equity_peak or balance >= _state.equity_peak:
        return False
    dd_pct = (_state.equity_peak - balance) / _state.equity_peak * 100
    threshold = _s.get_val("auto_reduce_dd_pct", 15.0)
    return dd_pct >= threshold


def get_drawdown_pct(balance: float) -> float:
    if not _state.equity_peak or balance >= _state.equity_peak:
        return 0.0
    return round((_state.equity_peak - balance) / _state.equity_peak * 100, 2)


def reset_weekly() -> None:
    _state.weekly_loss_usdt = 0.0
    _state.weekly_pnl_usdt  = 0.0
    _state.weekly_reset_ts  = datetime.now(timezone.utc).isoformat()
    _save()


# ── Metrics ───────────────────────────────────────────────────────────────────

def get_metrics() -> dict:
    trades = _state.trades
    if not trades:
        return {
            "total": 0, "wins": 0, "losses": 0, "win_rate": 0.0,
            "total_pnl": 0.0, "avg_win": 0.0, "avg_loss": 0.0,
            "sharpe": 0.0, "calmar": 0.0, "max_dd": 0.0,
            "consec_losses": 0, "size_halved": False,
        }
    scored = [t for t in trades if _trade_outcome(t) != "neutral"]
    wins   = [t for t in scored if _trade_outcome(t) == "win"]
    losses = [t for t in scored if _trade_outcome(t) == "loss"]
    pnls   = [_trade_net_pnl(t) for t in trades]
    total_pnl = sum(pnls)
    avg_win   = sum(_trade_net_pnl(t) for t in wins)   / len(wins)   if wins   else 0.0
    avg_loss  = sum(_trade_net_pnl(t) for t in losses) / len(losses) if losses else 0.0

    # Simplified Sharpe: mean PnL / std PnL
    if len(pnls) > 1:
        mean = total_pnl / len(pnls)
        std  = math.sqrt(sum((p - mean) ** 2 for p in pnls) / len(pnls))
        sharpe = round((mean / std) * math.sqrt(252) if std > 0 else 0.0, 2)
    else:
        sharpe = 0.0

    # Max drawdown from trade sequence
    peak = 0.0
    max_dd = 0.0
    equity = 0.0
    for p in pnls:
        equity += p
        if equity > peak:
            peak = equity
        dd = (peak - equity) / peak * 100 if peak > 0 else 0.0
        if dd > max_dd:
            max_dd = dd

    # Calmar = win_rate / max_dd (dimensionally consistent ratio for display)
    calmar = round(len(wins) / len(scored) / (max_dd / 100) if max_dd > 0 and scored else 0.0, 2)

    return {
        "total":         len(scored),
        "wins":          len(wins),
        "losses":        len(losses),
        "win_rate":      round(len(wins) / len(scored) * 100, 1) if scored else 0.0,
        "total_pnl":     round(total_pnl, 4),
        "avg_win":       round(avg_win, 4),
        "avg_loss":      round(avg_loss, 4),
        "sharpe":        sharpe,
        "calmar":        calmar,
        "max_dd":        round(max_dd, 2),
        "consec_losses": _state.consec_losses,
        "size_halved":   _state.size_halved,
        "total_fees":    round(_state.total_fees_usdt, 4),
    }


def get_recent_trades(n: int = 20) -> list[TradeRecord]:
    """Return the last *n* closed trades, newest last."""
    if n <= 0:
        return []
    return list(_state.trades[-n:])


def get_last_closed_trade(coin: str) -> TradeRecord | None:
    """Return the newest closed trade for *coin*, or None."""
    coin_u = str(coin or "").upper()
    if not coin_u:
        return None
    for trade in reversed(_state.trades):
        if str(trade.coin or "").upper() == coin_u:
            return trade
    return None


def get_recent_directional_loss_count(coin: str, direction: str, window_mins: int = 720) -> int:
    coin_u = str(coin or "").upper()
    direction_l = str(direction or "").lower()
    if not coin_u or direction_l not in {"long", "short"}:
        return 0
    now = datetime.now(timezone.utc)
    cutoff = max(float(window_mins or 0), 0.0) * 60.0
    count = 0
    for trade in reversed(_state.trades):
        try:
            closed_at = datetime.fromisoformat(str(trade.closed_at))
        except Exception:
            continue
        if cutoff > 0 and (now - closed_at).total_seconds() > cutoff:
            break
        if str(trade.coin or "").upper() == coin_u and str(trade.direction or "").lower() == direction_l:
            net = float(getattr(trade, "net_pnl_usdt", trade.pnl_usdt) or 0.0)
            if net < 0:
                count += 1
            else:
                break
    return count


def get_recent_closed_coins(window_mins: int = 240, limit: int = 12) -> list[str]:
    """Return unique recently closed coins, newest first."""
    if window_mins <= 0 or limit <= 0:
        return []
    now = datetime.now(timezone.utc)
    cutoff_secs = float(window_mins) * 60.0
    coins: list[str] = []
    seen: set[str] = set()
    for trade in reversed(_state.trades):
        try:
            closed_at = datetime.fromisoformat(str(trade.closed_at))
        except Exception:
            continue
        if (now - closed_at).total_seconds() > cutoff_secs:
            break
        coin_u = str(trade.coin or "").upper()
        if not coin_u or coin_u in seen:
            continue
        seen.add(coin_u)
        coins.append(coin_u)
        if len(coins) >= limit:
            break
    return coins


def get_win_streak() -> int:
    return _state.consec_wins


def get_win_streak_multiplier() -> float:
    """Returns size multiplier based on win streak boost setting."""
    import settings_store as _s
    if not _s.get_val("win_streak_boost_enabled", False):
        return 1.0
    threshold = int(_s.get_val("win_streak_threshold", 3))
    boost_pct = float(_s.get_val("win_streak_boost_pct", 25.0))
    if _state.consec_wins >= threshold:
        return round(1.0 + boost_pct / 100.0, 3)
    return 1.0


def get_daily_gain_usdt() -> float:
    return _state.daily_gain_usdt


def is_daily_profit_target_hit(balance: float) -> tuple[bool, float]:
    import settings_store as _s
    if not _s.get_val("daily_profit_target_enabled", False):
        return False, 0.0
    target_pct = _s.get_val("daily_profit_target_pct", 5.0)
    if balance <= 0:
        return False, 0.0
    gain_pct = (_state.daily_gain_usdt / balance) * 100
    return gain_pct >= target_pct, round(gain_pct, 2)


def reset_daily_gain():
    _state.daily_gain_usdt = 0.0
    _save()


def reset_session() -> None:
    """Reset all performance/risk tracking for a fresh session (open trades unaffected)."""
    # Keep the object identity of _state stable; mutate fields in-place so references remain valid.
    now = datetime.now(timezone.utc).isoformat()
    _state.trades = []
    _state.equity_peak = 0.0
    _state.weekly_loss_usdt = 0.0
    _state.weekly_pnl_usdt = 0.0
    _state.weekly_reset_ts = now
    _state.session_reset_ts = now
    _state.consec_losses = 0
    _state.size_halved = False
    _state.blackswan_halted = False
    _state.total_fees_usdt = 0.0
    _state.consec_wins = 0
    _state.daily_gain_usdt = 0.0
    _save()


def format_performance(balance: float) -> str:
    m  = get_metrics()
    dd = get_drawdown_pct(balance)
    wk_hit, wk_pct = is_weekly_cap_hit(balance)
    lines = [
        "📊 *Performance Dashboard*\n",
        f"  Trades:    `{m['total']}` | W: `{m['wins']}` L: `{m['losses']}` ({m['win_rate']}%)",
        f"  Total PnL: `${m['total_pnl']:+.4f}`",
        f"  Avg Win:   `${m['avg_win']:+.4f}` | Avg Loss: `${m['avg_loss']:+.4f}`",
        f"  Sharpe:    `{m['sharpe']}` | Calmar: `{m['calmar']}`",
        f"  Max DD:    `{m['max_dd']}%` | Current DD: `{dd}%`",
        f"  Weekly loss: `${_state.weekly_loss_usdt:.2f}` "
        f"({'⚠️ CAP HIT' if wk_hit else f'{wk_pct:.1f}% of cap'})",
        f"  Fees paid: `${m['total_fees']:.4f}`",
        f"  Size halved: {'⚠️ YES (recovering)' if m['size_halved'] else '✅ NO'}",
        f"  🚨 Blackswan halt: {'YES' if is_blackswan_halted() else 'NO'}",
    ]
    return "\n".join(lines)


_load()

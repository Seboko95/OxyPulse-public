"""
OxyPulse â€” category_tracker.py  v3
Per-engine win/loss/PnL tracking for all OxyPulse category engines.

V6.1: includes the intelligence pack (ENMA, HACHIMAN, HAYATE, KAGUTSUCHI,
YATAGARASU, RYUJIN, TENSEI, IBUKI, ONRYO, KANNON, GENBU).
Persists to data/category_stats.json.
Auto-resets today counters at midnight UTC.
"""
from __future__ import annotations
import json
import logging
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone, date
from pathlib import Path
import settings_store as _settings

logger = logging.getLogger("OxyPulse.CategoryTracker")
_PATH  = Path("data/category_stats.json")
_duplicate_close_warnings: set[tuple] = set()

ENGINE_NAMES = [
    "RYUU", "RAIJIN", "KAGE", "ARASHI", "MUSASHI",
    "SHOGUN", "TENJIN", "IZANAGI",
    "SUSANOO", "FUJIN", "AMATERASU",
    "TITAN", "RONIN",
    "OROCHI", "TSUNAMI", "SUIJIN", "RAIJUU",
    "ENMA", "HACHIMAN", "HAYATE", "KAGUTSUCHI", "YATAGARASU", "RYUJIN",
    "TENSEI", "IBUKI", "ONRYO", "KANNON", "GENBU",
]

ENGINE_EMOJI = {
    "RYUU":      "🐉",
    "RAIJIN":    "⚡",
    "KAGE":      "🌑",
    "ARASHI":    "🌪️",
    "MUSASHI":   "⚔️",
    "SHOGUN":    "🐯",
    "TENJIN":    "🌅",
    "IZANAGI":   "🌌",
    "SUSANOO":   "⛩️",
    "FUJIN":     "💨",
    "AMATERASU": "☀️",
    "TITAN":     "🛕",
    "RONIN":     "🏹",
    "OROCHI":    "🐍",
    "TSUNAMI":   "🌊",
    "SUIJIN":    "💧",
    "RAIJUU":    "⚡",
    "ENMA":      "⚖️",
    "HACHIMAN":  "🏯",
    "HAYATE":    "🍃",
    "KAGUTSUCHI":"🔥",
    "YATAGARASU":"🦅",
    "RYUJIN":    "🐲",
    "TENSEI":    "♻️",
    "IBUKI":     "🌬️",
    "ONRYO":     "👻",
    "KANNON":    "🛟",
    "GENBU":     "🐢",
}

ENGINE_FULLNAME = {
    "RYUU":      "RYUU — Trend",
    "RAIJIN":    "RAIJIN — Momentum",
    "KAGE":      "KAGE — Reversal",
    "ARASHI":    "ARASHI — Breakout",
    "MUSASHI":   "MUSASHI — Structure",
    "SHOGUN":    "SHOGUN — CRT/SMC/ICT",
    "TENJIN":    "TENJIN — Swing",
    "IZANAGI":   "IZANAGI — Composite Signal",
    "SUSANOO":   "SUSANOO — Funding Squeeze",
    "FUJIN":     "FUJIN — Order Flow",
    "AMATERASU": "AMATERASU — MTF Confluence",
    "TITAN":     "TITAN — Large-Cap Mean Reversion",
    "RONIN":     "RONIN — Large-Cap Breakout Momentum",
    "OROCHI":    "OROCHI — Liquid Trend Pullback",
    "TSUNAMI":   "TSUNAMI — Liquidation Cascade",
    "SUIJIN":    "SUIJIN — VWAP Reversion",
    "RAIJUU":    "RAIJUU — OI Divergence",
    "ENMA":      "ENMA — Cluster Selector",
    "HACHIMAN":  "HACHIMAN — Battlefield Selector",
    "HAYATE":    "HAYATE — Relative Strength Rotation",
    "KAGUTSUCHI":"KAGUTSUCHI — Order-Flow Confirmation",
    "YATAGARASU":"YATAGARASU — Premium Confluence Hunter",
    "RYUJIN":    "RYUJIN — Flow Pressure Hunter",
    "TENSEI":    "TENSEI — Safe Re-entry",
    "IBUKI":     "IBUKI — Compression Expansion",
    "ONRYO":     "ONRYO — Funding + OI Pressure",
    "KANNON":    "KANNON — Recovery Guardian",
    "GENBU":     "GENBU — Advisory Governor",
}

ENGINE_STYLE = {
    "RYUU":      "Trend-follow",
    "RAIJIN":    "Momentum",
    "KAGE":      "Reversal",
    "ARASHI":    "Breakout",
    "MUSASHI":   "Structure",
    "SHOGUN":    "Smart Money",
    "TENJIN":    "Swing (4H)",
    "IZANAGI":   "Composite Scorer",
    "SUSANOO":   "Funding Fade",
    "FUJIN":     "Order Flow",
    "AMATERASU": "MTF Confluence",
    "TITAN":     "Mean Reversion",
    "RONIN":     "Large-Cap Breakout",
    "OROCHI":    "Trend Pullback",
    "TSUNAMI":   "Liq Cascade",
    "SUIJIN":    "VWAP Reversion",
    "RAIJUU":    "OI Divergence",
    "ENMA":      "Cluster Selector",
    "HACHIMAN":  "Selector",
    "HAYATE":    "RS Rotation",
    "KAGUTSUCHI":"Flow Confirm",
    "YATAGARASU":"Premium Confluence",
    "RYUJIN":    "Flow Pressure",
    "TENSEI":    "Safe Re-entry",
    "IBUKI":     "Vol Compression",
    "ONRYO":     "Funding + OI",
    "KANNON":    "Recovery Guard",
    "GENBU":     "Advisory Governor",
}

ENGINE_PUBLIC_NAME = {}


def engine_public_name(engine: str) -> str:
    engine = str(engine or "").upper()
    return ENGINE_PUBLIC_NAME.get(engine, engine)


def engine_display_name(engine: str) -> str:
    engine = str(engine or "").upper()
    public = engine_public_name(engine)
    return f"{public} ({engine})" if public and public != engine else engine


def engine_log_name(engine: str) -> str:
    engine = str(engine or "").upper()
    public = engine_public_name(engine)
    return f"{public}/{engine}" if public and public != engine else engine


@dataclass
class EngineStats:
    name:            str
    trades:          list  = field(default_factory=list)
    today_wins:      int   = 0
    today_losses:    int   = 0
    today_pnl:       float = 0.0
    week_wins:       int   = 0
    week_losses:     int   = 0
    week_pnl:        float = 0.0
    total_wins:      int   = 0
    total_losses:    int   = 0
    total_pnl:       float = 0.0
    last_reset_day:  str   = ""
    last_reset_week: str   = ""


_stats: dict[str, EngineStats] = {
    n: EngineStats(name=n) for n in ENGINE_NAMES
}


# â”€â”€ Persistence â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def _save() -> None:
    try:
        _PATH.parent.mkdir(parents=True, exist_ok=True)
        data = {name: asdict(s) for name, s in _stats.items()}
        _PATH.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
    except Exception as e:
        logger.warning("CategoryTracker save failed: %s", e)


def _load() -> None:
    global _stats
    try:
        if _PATH.exists():
            raw = json.loads(_PATH.read_text(encoding="utf-8"))
            for name in ENGINE_NAMES:
                if name in raw:
                    d = raw[name]
                    _stats[name] = EngineStats(
                        name            = name,
                        trades          = d.get("trades", []),
                        today_wins      = int(d.get("today_wins", 0)),
                        today_losses    = int(d.get("today_losses", 0)),
                        today_pnl       = float(d.get("today_pnl", 0.0)),
                        week_wins       = int(d.get("week_wins", 0)),
                        week_losses     = int(d.get("week_losses", 0)),
                        week_pnl        = float(d.get("week_pnl", 0.0)),
                        total_wins      = int(d.get("total_wins", 0)),
                        total_losses    = int(d.get("total_losses", 0)),
                        total_pnl       = float(d.get("total_pnl", 0.0)),
                        last_reset_day  = str(d.get("last_reset_day", "")),
                        last_reset_week = str(d.get("last_reset_week", "")),
                    )
            logger.info("CategoryTracker loaded.")
    except Exception as e:
        logger.warning("CategoryTracker load failed: %s", e)


# â”€â”€ Reset logic â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def _check_resets(engine: str) -> None:
    s     = _stats[engine]
    dt    = datetime.now(timezone.utc)
    today = str(dt.date())   # UTC date â€” consistent with week_key below
    week_key = f"{dt.isocalendar()[0]}-W{dt.isocalendar()[1]:02d}"
    if s.last_reset_day != today:
        s.today_wins = s.today_losses = 0
        s.today_pnl  = 0.0
        s.last_reset_day = today
    if s.last_reset_week != week_key:
        s.week_wins = s.week_losses = 0
        s.week_pnl  = 0.0
        s.last_reset_week = week_key


def reset_session() -> None:
    """Reset all per-engine performance stats for a fresh session (open trades unaffected)."""
    _duplicate_close_warnings.clear()
    dt = datetime.now(timezone.utc)
    today = str(dt.date())
    week_key = f"{dt.isocalendar()[0]}-W{dt.isocalendar()[1]:02d}"
    for name in ENGINE_NAMES:
        s = _stats.get(name)
        if not s:
            continue
        s.trades = []
        s.today_wins = 0
        s.today_losses = 0
        s.today_pnl = 0.0
        s.week_wins = 0
        s.week_losses = 0
        s.week_pnl = 0.0
        s.total_wins = 0
        s.total_losses = 0
        s.total_pnl = 0.0
        s.last_reset_day = today
        s.last_reset_week = week_key
    _save()
    logger.info("CategoryTracker session reset executed.")


# â”€â”€ Recording â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def _close_key(
    engine: str,
    coin: str,
    direction: str,
    entry: float,
    size_usdt: float,
    opened_at: str,
) -> tuple:
    return (
        str(engine or "").upper(),
        str(coin or "").upper(),
        str(direction or "").lower(),
        round(float(entry or 0.0), 8),
        round(float(size_usdt or 0.0), 4),
        str(opened_at or "").strip(),
    )


def _is_duplicate_close(s: EngineStats, rec: dict) -> bool:
    key = _close_key(
        rec.get("engine", ""),
        rec.get("coin", ""),
        rec.get("direction", ""),
        float(rec.get("entry", 0.0) or 0.0),
        float(rec.get("size_usdt", 0.0) or 0.0),
        str(rec.get("opened_at", "") or ""),
    )
    for prev in reversed(s.trades[-500:]):
        prev_key = _close_key(
            prev.get("engine", ""),
            prev.get("coin", ""),
            prev.get("direction", ""),
            float(prev.get("entry", 0.0) or 0.0),
            float(prev.get("size_usdt", 0.0) or 0.0),
            str(prev.get("opened_at", "") or ""),
        )
        if prev_key == key:
            if key not in _duplicate_close_warnings:
                logger.warning(
                    "CategoryTracker: suppressed duplicate close %s %s %s entry=%.8f size=%.4f opened_at=%s",
                    rec.get("engine", ""),
                    rec.get("coin", ""),
                    rec.get("direction", ""),
                    float(rec.get("entry", 0.0) or 0.0),
                    float(rec.get("size_usdt", 0.0) or 0.0),
                    str(rec.get("opened_at", "") or ""),
                )
                _duplicate_close_warnings.add(key)
            return True
    return False


def _is_neutral_trade(trade: dict) -> bool:
    reason = str(trade.get("exit_reason", "") or "").lower()
    if reason != "slot_rotation":
        return False
    gross = float(trade.get("gross_pnl_usdt", trade.get("pnl_usdt", 0.0)) or 0.0)
    return abs(gross) < 1e-9


def _trade_outcome(trade: dict) -> str:
    if _is_neutral_trade(trade):
        return "neutral"
    _is_sl = any(r in str(trade.get("exit_reason", "") or "") for r in ("sl", "tsl"))
    return "win" if float(trade.get("pnl_usdt", 0.0) or 0.0) > 0 and not _is_sl else "loss"


def record_trade(
    engine:      str,
    coin:        str,
    direction:   str,
    entry:       float,
    exit_price:  float,
    size_usdt:   float,
    pnl_usdt:    float,
    vote_score:  int = 2,
    exit_reason: str = "tp",
    fee_usdt:    float = 0.0,
    opened_at:   str = "",
    closed_at:   str = "",
    metadata:    dict | None = None,
) -> None:
    engine = engine.upper()
    if engine not in _stats:
        logger.warning("CategoryTracker: unknown engine %s", engine)
        return
    _check_resets(engine)
    s = _stats[engine]
    gross_pnl_usdt = round(float(pnl_usdt or 0.0), 4)
    fee_usdt = round(float(fee_usdt or 0.0), 6)
    net_pnl_usdt = round(gross_pnl_usdt - fee_usdt, 4)
    close_ts = str(closed_at or "").strip() or datetime.now(timezone.utc).isoformat()
    opened_ts = str(opened_at or "").strip()
    rec = {
        "coin": coin, "direction": direction, "engine": engine,
        "entry": entry, "exit_price": exit_price,
        "size_usdt": size_usdt,
        "pnl_usdt": net_pnl_usdt,
        "gross_pnl_usdt": gross_pnl_usdt,
        "fee_usdt": fee_usdt,
        "vote_score": vote_score, "exit_reason": exit_reason,
        "opened_at": opened_ts,
        "closed_at": close_ts,
    }
    if opened_ts:
        try:
            opened_dt = datetime.fromisoformat(opened_ts)
            closed_dt = datetime.fromisoformat(close_ts)
            if opened_dt < closed_dt:
                rec["holding_minutes"] = round((closed_dt - opened_dt).total_seconds() / 60.0, 2)
        except Exception:
            pass
    if metadata:
        rec.update(dict(metadata))
    if _is_duplicate_close(s, rec):
        return
    s.trades.append(rec)
    if len(s.trades) > 500:
        s.trades = s.trades[-500:]
    # A zero-gross slot rotation is administrative, not a strategy win/loss.
    # Fees still flow into PnL totals, but W/L counters exclude the close.
    outcome = _trade_outcome(rec)
    if outcome == "win":
        s.today_wins += 1; s.week_wins += 1; s.total_wins += 1
    elif outcome == "loss":
        s.today_losses += 1; s.week_losses += 1; s.total_losses += 1
    s.today_pnl += net_pnl_usdt; s.week_pnl += net_pnl_usdt; s.total_pnl += net_pnl_usdt
    _save()
    try:
        if (
            bool(_settings.get_val("shadow_rehab_enabled", True))
            and str(_settings.get_engine_mode(engine)).lower() == "probation"
        ):
            import shadow_rehab_tracker as _shadow_rehab
            _shadow_rehab.record_probation_close(
                engine=engine,
                symbol=coin,
                side=direction,
                entry_price=float(entry or 0.0),
                exit_price=float(exit_price or 0.0),
                size_usdt=float(size_usdt or 0.0),
                pnl_usdt=float(net_pnl_usdt or 0.0),
                fee_usdt=float(fee_usdt or 0.0),
                opened_at=opened_ts,
                closed_at=close_ts,
            )
    except Exception:
        logger.debug("CategoryTracker: shadow rehab mirror failed for %s %s", engine, coin)
    logger.info("[%s] %s %s %s pnl=$%.4f fee=$%.4f votes=%d/3 reason=%s",
        engine_log_name(engine), outcome.upper(),
        coin, direction.upper(), net_pnl_usdt, fee_usdt, vote_score, exit_reason)


# â”€â”€ Getters â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def get_engine_stats(engine: str) -> dict:
    engine = engine.upper()
    if engine not in _stats:
        return {}
    _check_resets(engine)
    s      = _stats[engine]
    today  = s.today_wins + s.today_losses
    week   = s.week_wins  + s.week_losses
    total  = s.total_wins + s.total_losses
    trades = s.trades
    v3     = [t["pnl_usdt"] for t in trades if t.get("vote_score") == 3]
    v2     = [t["pnl_usdt"] for t in trades if t.get("vote_score") == 2]
    all_p  = [t["pnl_usdt"] for t in trades]
    return {
        "name":          s.name,
        "display_name":  engine_display_name(engine),
        "log_name":      engine_log_name(engine),
        "fullname":      ENGINE_FULLNAME.get(engine, engine),
        "style":         ENGINE_STYLE.get(engine, ""),
        "emoji":         ENGINE_EMOJI.get(engine, "🤖"),
        "today_wins":    s.today_wins,   "today_losses":  s.today_losses,
        "today_pnl":     round(s.today_pnl, 4),
        "today_wr":      round(s.today_wins / today * 100, 1) if today else 0.0,
        "week_wins":     s.week_wins,    "week_losses":   s.week_losses,
        "week_pnl":      round(s.week_pnl, 4),
        "week_wr":       round(s.week_wins / week * 100, 1) if week else 0.0,
        "total_wins":    s.total_wins,   "total_losses":  s.total_losses,
        "total_pnl":     round(s.total_pnl, 4),
        "total_wr":      round(s.total_wins / total * 100, 1) if total else 0.0,
        "trade_count":   len(trades),
        "avg_3vote":     round(sum(v3) / len(v3), 4) if v3 else 0.0,
        "avg_2vote":     round(sum(v2) / len(v2), 4) if v2 else 0.0,
        "count_3vote":   len(v3),        "count_2vote": len(v2),
        "best_trade":    round(max(all_p), 4) if all_p else 0.0,
        "worst_trade":   round(min(all_p), 4) if all_p else 0.0,
    }


def get_combined_stats() -> dict:
    tw = tl = ww = wl = aw = al = 0
    tp = wp = ap = 0.0
    best_engine = worst_engine = None
    best_pnl    = float("-inf")
    worst_pnl   = float("inf")
    for name in ENGINE_NAMES:
        _check_resets(name)
        s = _stats[name]
        tw += s.today_wins;  tl += s.today_losses;  tp += s.today_pnl
        ww += s.week_wins;   wl += s.week_losses;   wp += s.week_pnl
        aw += s.total_wins;  al += s.total_losses;  ap += s.total_pnl
        if not s.trades:
            continue
        if s.total_pnl > best_pnl:
            best_pnl = s.total_pnl; best_engine = name
        if s.total_pnl < worst_pnl:
            worst_pnl = s.total_pnl; worst_engine = name
    today = tw + tl; week = ww + wl; total = aw + al
    em    = ENGINE_EMOJI
    return {
        "today_wins": tw,    "today_losses": tl,
        "today_pnl":  round(tp, 4),
        "today_wr":   round(tw / today * 100, 1) if today else 0.0,
        "week_wins":  ww,    "week_losses":  wl,
        "week_pnl":   round(wp, 4),
        "week_wr":    round(ww / week * 100, 1) if week else 0.0,
        "total_wins": aw,    "total_losses": al,
        "total_pnl":  round(ap, 4),
        "total_wr":   round(aw / total * 100, 1) if total else 0.0,
        "best_engine":  f"{em.get(best_engine,'')} {engine_display_name(best_engine)}  ${best_pnl:+.2f}" if best_engine else "â€”",
        "worst_engine": f"{em.get(worst_engine,'')} {engine_display_name(worst_engine)} ${worst_pnl:+.2f}" if worst_engine else "â€”",
    }


# â”€â”€ Win Rate Monitor â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def _trade_is_win(trade: dict) -> bool:
    """Consistent win classification â€” matches record_trade() logic."""
    return _trade_outcome(trade) == "win"


def get_rolling_win_rate(engine: str, n: int = 20) -> dict:
    """Rolling win rate for last N closed trades of a single engine."""
    engine = engine.upper()
    if engine not in _stats:
        return {"win_rate": 0.0, "wins": 0, "losses": 0, "trades": 0}
    scored = [t for t in _stats[engine].trades if _trade_outcome(t) != "neutral"]
    recent = scored[-n:]
    if not recent:
        return {"win_rate": 0.0, "wins": 0, "losses": 0, "trades": 0}
    wins = sum(1 for t in recent if _trade_is_win(t))
    return {
        "win_rate": round(wins / len(recent) * 100, 1),
        "wins":     wins,
        "losses":   len(recent) - wins,
        "trades":   len(recent),
    }


def get_recent_trades(engine: str, n: int = 20) -> list[dict]:
    engine = engine.upper()
    if engine not in _stats:
        return []
    return list(_stats[engine].trades[-n:])


def _trade_net_pnl(trade: dict) -> float:
    if "net_pnl_usdt" in trade:
        return float(trade.get("net_pnl_usdt", 0.0) or 0.0)
    if "gross_pnl_usdt" in trade:
        return float(trade.get("pnl_usdt", 0.0) or 0.0)
    return float(trade.get("pnl_usdt", 0.0) or 0.0) - float(trade.get("fee_usdt", 0.0) or 0.0)


def _parse_trade_dt(value) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        dt = datetime.fromisoformat(text)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def _trade_identity(engine: str, trade: dict) -> tuple:
    closed = _parse_trade_dt(trade.get("closed_at"))
    bucket = closed.replace(microsecond=0).isoformat() if closed else str(trade.get("closed_at", ""))
    return (
        str(engine or "").upper(),
        str(trade.get("coin", "")).upper(),
        str(trade.get("direction", "")).upper(),
        round(float(trade.get("entry", 0.0) or 0.0), 8),
        round(float(trade.get("exit_price", 0.0) or 0.0), 8),
        round(float(trade.get("size_usdt", 0.0) or 0.0), 4),
        str(trade.get("exit_reason", "")),
        bucket,
    )


def get_daily_report_snapshot(now: datetime | None = None, open_positions: int | None = None) -> dict:
    """Recompute today's Telegram report from persisted close records."""
    ref = now or datetime.now(timezone.utc)
    if ref.tzinfo is None:
        ref = ref.replace(tzinfo=timezone.utc)
    report_day = ref.astimezone(timezone.utc).date()
    seen: set[tuple] = set()
    rows: list[tuple[str, dict]] = []
    duplicate_count = 0
    for name in ENGINE_NAMES:
        s = _stats.get(name)
        if not s:
            continue
        for trade in list(s.trades or []):
            closed = _parse_trade_dt(trade.get("closed_at"))
            if closed is None or closed.date() != report_day:
                continue
            ident = _trade_identity(name, trade)
            if ident in seen:
                duplicate_count += 1
                continue
            seen.add(ident)
            rows.append((name, trade))

    wins = sum(1 for _, row in rows if _trade_outcome(row) == "win")
    losses = sum(1 for _, row in rows if _trade_outcome(row) == "loss")
    scored = wins + losses
    gross_pnl = sum(float(row.get("gross_pnl_usdt", row.get("pnl_usdt", 0.0)) or 0.0) for _, row in rows)
    fee_usdt = sum(float(row.get("fee_usdt", 0.0) or 0.0) for _, row in rows)
    net_pnl = sum(_trade_net_pnl(row) for _, row in rows)
    exit_counts: dict[str, int] = {}
    engine_stats: dict[str, dict] = {}
    for engine, row in rows:
        reason = str(row.get("exit_reason", "unknown") or "unknown")
        exit_counts[reason] = exit_counts.get(reason, 0) + 1
        bucket = engine_stats.setdefault(engine, {"trades": 0, "wins": 0, "losses": 0, "net_pnl_usdt": 0.0, "fee_usdt": 0.0})
        bucket["trades"] += 1
        outcome = _trade_outcome(row)
        if outcome == "win":
            bucket["wins"] += 1
        elif outcome == "loss":
            bucket["losses"] += 1
        bucket["net_pnl_usdt"] += _trade_net_pnl(row)
        bucket["fee_usdt"] += float(row.get("fee_usdt", 0.0) or 0.0)

    for bucket in engine_stats.values():
        bucket["net_pnl_usdt"] = round(float(bucket["net_pnl_usdt"]), 4)
        bucket["fee_usdt"] = round(float(bucket["fee_usdt"]), 6)

    early_invalidations = sum(count for reason, count in exit_counts.items() if "early_invalidation" in reason)
    tp_hits = sum(count for reason, count in exit_counts.items() if "tp" in reason.lower())
    return {
        "date": str(report_day),
        "timezone": "UTC",
        "total_trades": len(rows),
        "wins": wins,
        "losses": losses,
        "win_rate": round(wins / scored * 100.0, 1) if scored else 0.0,
        "gross_pnl_usdt": round(gross_pnl, 4),
        "fee_usdt": round(fee_usdt, 6),
        "net_pnl_usdt": round(net_pnl, 4),
        "exit_counts": dict(sorted(exit_counts.items())),
        "early_invalidation_count": early_invalidations,
        "tp_hit_count": tp_hits,
        "engine_stats": dict(sorted(engine_stats.items())),
        "duplicate_trade_count": duplicate_count,
        "open_positions": open_positions,
    }


def get_engine_recent_performance(engine: str, short_n: int = 10, long_n: int = 30) -> dict:
    """Fee-aware recent performance used for soft role weighting and diagnostics."""
    engine = str(engine or "").upper()
    if engine not in _stats:
        return {
            "engine": engine,
            "last10_net": 0.0,
            "last30_net": 0.0,
            "last10_wr": 0.0,
            "last30_wr": 0.0,
            "loss_streak": 0,
            "avg_gross_pnl": 0.0,
            "avg_fee": 0.0,
            "avg_net_pnl": 0.0,
            "fee_to_profit_ratio": 0.0,
            "tiny_win_rate": 0.0,
            "duplicate_entry_rate": 0.0,
            "trade_count": 0,
        }
    trades = list(_stats[engine].trades or [])
    short = trades[-max(int(short_n or 10), 1):]
    long = trades[-max(int(long_n or 30), 1):]

    def _wr(rows: list[dict]) -> float:
        rows = [row for row in rows if _trade_outcome(row) != "neutral"]
        if not rows:
            return 0.0
        wins = sum(1 for row in rows if _trade_is_win(row))
        return round(wins / len(rows) * 100.0, 1)

    def _avg(values: list[float]) -> float:
        return round(sum(values) / len(values), 4) if values else 0.0

    gross_vals = [
        float(row.get("gross_pnl_usdt", row.get("pnl_usdt", 0.0)) or 0.0)
        for row in long
    ]
    fees = [float(row.get("fee_usdt", 0.0) or 0.0) for row in long]
    nets = [_trade_net_pnl(row) for row in long]
    gross_profit = sum(max(v, 0.0) for v in gross_vals)
    fee_to_profit = (sum(fees) / gross_profit) if gross_profit > 0 else (1.0 if sum(fees) > 0 else 0.0)
    tiny_wins = [
        row for row in long
        if _trade_net_pnl(row) > 0 and abs(_trade_net_pnl(row)) < 0.05
    ]
    dupes = [
        row for row in long
        if int(row.get("confirmation_count", 0) or 0) > 0
        or len(row.get("confirmed_by", []) or []) > 0
        or len(row.get("confirming_engines", []) or []) > 0
    ]
    return {
        "engine": engine,
        "last10_net": round(sum(_trade_net_pnl(row) for row in short), 4),
        "last30_net": round(sum(nets), 4),
        "last10_wr": _wr(short),
        "last30_wr": _wr(long),
        "loss_streak": get_loss_streak(engine),
        "avg_gross_pnl": _avg(gross_vals),
        "avg_fee": _avg(fees),
        "avg_net_pnl": _avg(nets),
        "fee_to_profit_ratio": round(fee_to_profit, 4),
        "tiny_win_rate": round(len(tiny_wins) / len(long) * 100.0, 1) if long else 0.0,
        "duplicate_entry_rate": round(len(dupes) / len(long) * 100.0, 1) if long else 0.0,
        "trade_count": len(trades),
    }


def get_loss_streak(engine: str) -> int:
    engine = engine.upper()
    if engine not in _stats:
        return 0
    streak = 0
    for trade in reversed(_stats[engine].trades):
        outcome = _trade_outcome(trade)
        if outcome == "neutral":
            continue
        if outcome == "win":
            break
        streak += 1
    return streak


def get_engine_guard_status(engine: str, n: int = 20) -> dict:
    stats = get_engine_stats(engine)
    roll = get_rolling_win_rate(engine, n)
    return {
        "engine": engine.upper(),
        "today_pnl": float(stats.get("today_pnl", 0.0) or 0.0),
        "today_wr": float(stats.get("today_wr", 0.0) or 0.0),
        "trade_count": int(stats.get("trade_count", 0) or 0),
        "loss_streak": get_loss_streak(engine),
        "roll_win_rate": float(roll.get("win_rate", 0.0) or 0.0),
        "roll_trades": int(roll.get("trades", 0) or 0),
        "roll_wins": int(roll.get("wins", 0) or 0),
        "roll_losses": int(roll.get("losses", 0) or 0),
    }


def get_global_rolling_win_rate(n: int = 20) -> dict:
    """Rolling win rate across ALL engines combined, using the last N global trades."""
    all_trades = []
    for name in ENGINE_NAMES:
        all_trades.extend(t for t in _stats[name].trades if _trade_outcome(t) != "neutral")
    all_trades.sort(key=lambda t: t.get("closed_at", ""))
    recent = all_trades[-n:]
    if not recent:
        return {"win_rate": 0.0, "wins": 0, "losses": 0, "trades": 0}
    wins = sum(1 for t in recent if _trade_is_win(t))
    return {
        "win_rate": round(wins / len(recent) * 100, 1),
        "wins":     wins,
        "losses":   len(recent) - wins,
        "trades":   len(recent),
    }


def _wr_trend_arrow(engine: str) -> str:
    """UP improving / DOWN declining / FLAT stable compared to last-20."""
    trades = _stats.get(engine.upper(), EngineStats(name=engine)).trades
    trades = [t for t in trades if _trade_outcome(t) != "neutral"]
    if len(trades) < 10:
        return "FLAT"
    last10 = trades[-10:]
    last20 = trades[-20:] if len(trades) >= 20 else trades
    wr10 = sum(1 for t in last10 if _trade_is_win(t)) / len(last10) * 100
    wr20 = sum(1 for t in last20 if _trade_is_win(t)) / len(last20) * 100
    if wr10 >= wr20 + 5:
        return "UP"
    if wr10 <= wr20 - 5:
        return "DOWN"
    return "FLAT"


def _wr_bar(pct: float, width: int = 10) -> str:
    """Simple ASCII progress bar."""
    filled = round(pct / 100 * width)
    return "#" * filled + "-" * (width - filled)


def format_winrate_report() -> str:
    """Full win rate monitor for /winrate command â€” rolling, today, all-time + trend."""
    g20  = get_global_rolling_win_rate(20)
    g50  = get_global_rolling_win_rate(50)
    comb = get_combined_stats()

    pct_bar = _wr_bar(g20["win_rate"])
    lines = [
        "*Win Rate Monitor*",
        "",
        f"  `[{pct_bar}] {g20['win_rate']}%`",
        "",
        "*Overall (all engines)*",
        f"  Roll-20:  `{g20['win_rate']}%`  ({g20['wins']}W / {g20['losses']}L  of {g20['trades']})",
        f"  Roll-50:  `{g50['win_rate']}%`  ({g50['wins']}W / {g50['losses']}L  of {g50['trades']})",
        f"  Today:    `{comb['today_wr']}%`  ({comb['today_wins']}W / {comb['today_losses']}L)",
        f"  Week:     `{comb['week_wr']}%`  ({comb['week_wins']}W / {comb['week_losses']}L)",
        f"  All-time: `{comb['total_wr']}%`  ({comb['total_wins']}W / {comb['total_losses']}L)",
        "",
        "*Per Engine* _(active only)_",
        "`Engine       Roll-20  Today  All-time  Trend`",
    ]

    active = False
    for name in ENGINE_NAMES:
        if not _settings.get_val(f"engine_{name.lower()}_enabled", True):
            continue
        s     = get_engine_stats(name)
        total = s.get("trade_count", 0)
        if total == 0:
            continue
        active = True
        roll  = get_rolling_win_rate(name, 20)
        trend = _wr_trend_arrow(name)
        em    = ENGINE_EMOJI.get(name, "")
        lines.append(
            f"  {em} `{name:<9}`  "
            f"`{roll['win_rate']:>5.1f}%`  "
            f"`{s['today_wr']:>5.1f}%`  "
            f"`{s['total_wr']:>5.1f}%`  {trend}"
        )

    if not active:
        lines.append("  _No enabled engines have closed trades yet._")

    lines += [
        "",
        "_Legend: UP improving  DOWN declining  FLAT stable_",
        "_Roll-20 = last 20 closed trades (rolling window)_",
    ]
    return "\n".join(lines)


def get_winrate_alert_status() -> dict:
    """
    Returns current rolling win rate info for alert checks.
    Used by bot.py background job to decide whether to send an alert.
    """
    g20 = get_global_rolling_win_rate(20)
    worst_engine = None
    worst_wr     = 100.0
    for name in ENGINE_NAMES:
        s     = _stats[name]
        total = s.total_wins + s.total_losses
        if total < 5:
            continue  # not enough data
        roll = get_rolling_win_rate(name, 20)
        if roll["trades"] >= 5 and roll["win_rate"] < worst_wr:
            worst_wr     = roll["win_rate"]
            worst_engine = name
    return {
        "global_rolling_wr": g20["win_rate"],
        "global_trades":     g20["trades"],
        "worst_engine":      worst_engine,
        "worst_engine_wr":   worst_wr if worst_engine else 0.0,
    }


# â”€â”€ Formatters â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def format_engine_report(engine: str) -> str:
    s = get_engine_stats(engine)
    if not s:
        return f"Unknown engine: {engine}"
    return (
        f"{s['emoji']} *{s['fullname']} Report*\n\n"
        f"  Today:    `{s['today_wins']}W / {s['today_losses']}L` "
        f"({s['today_wr']}%)  `${s['today_pnl']:+.4f}`\n"
        f"  Week:     `{s['week_wins']}W / {s['week_losses']}L` "
        f"({s['week_wr']}%)  `${s['week_pnl']:+.4f}`\n"
        f"  All time: `{s['total_wins']}W / {s['total_losses']}L` "
        f"({s['total_wr']}%)  `${s['total_pnl']:+.4f}`\n\n"
        f"  ⭐⭐⭐ Avg (3/3): `${s['avg_3vote']:+.4f}` ({s['count_3vote']} trades)\n"
        f"  ⭐⭐▫ Avg (2/3): `${s['avg_2vote']:+.4f}` ({s['count_2vote']} trades)\n\n"
        f"  Best:  `${s['best_trade']:+.4f}` | Worst: `${s['worst_trade']:+.4f}`\n"
        f"  Total trades: `{s['trade_count']}`"
    )


def format_combined_report() -> str:
    s = get_combined_stats()
    daily = get_daily_report_snapshot()
    _chart = "\U0001f4ca"
    _trophy = "\U0001f3c6"
    _down = "\U0001f4c9"
    top_exits = ", ".join(
        f"{reason}:{count}" for reason, count in list((daily.get("exit_counts") or {}).items())[:5]
    ) or "none"
    lines = [
        f"{_chart} *Combined Report \u2014 All Engines*\n",
        f"  Today:    `{daily['wins']}W / {daily['losses']}L` "
        f"({daily['win_rate']}%)  `${daily['net_pnl_usdt']:+.4f}` net",
        f"  Fees:     `${daily['fee_usdt']:+.6f}` | Gross: `${daily['gross_pnl_usdt']:+.4f}`",
        f"  Exits:    early invalidations `{daily['early_invalidation_count']}` | TP hits `{daily['tp_hit_count']}`",
        f"  Reasons:  `{top_exits}`",
        f"  Week:     `{s['week_wins']}W / {s['week_losses']}L` "
        f"({s['week_wr']}%)  `${s['week_pnl']:+.4f}`",
        f"  All time: `{s['total_wins']}W / {s['total_losses']}L` "
        f"({s['total_wr']}%)  `${s['total_pnl']:+.4f}`",
        "",
        f"  {_trophy} Best:  {s['best_engine']}",
        f"  {_down} Worst: {s['worst_engine']}",
    ]
    return "\n".join(lines)


_load()


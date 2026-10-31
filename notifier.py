"""
OxyPulse — notifier.py  v3
All Telegram alert types.

v3:
  • leverage parameter properly threaded through alert_stop_loss and
    alert_take_profit so net PnL shown is accurate.
  • alert_position_closed_by_exchange() — clean message when OKX algo fires.
  • alert_trade_executed includes SL/TP confirmation line.
"""
from __future__ import annotations
import os
import time
import logging
import json
import httpx
from pathlib import Path
from threading import RLock
from dotenv import load_dotenv

load_dotenv(override=True)
logger = logging.getLogger("OxyPulse.Notifier")

TOKEN   = os.getenv("TELEGRAM_TOKEN", "") or os.getenv("TELEGRAM_BOT_TOKEN", "")
CHAT_ID = os.getenv("OWNER_TELEGRAM_ID", "")
BASE    = f"https://api.telegram.org/bot{TOKEN}/sendMessage"

ALERT_TRADE  = os.getenv("ALERT_ON_TRADE",       "true").lower() == "true"
ALERT_SL     = os.getenv("ALERT_ON_STOP_LOSS",   "true").lower() == "true"
ALERT_TP     = os.getenv("ALERT_ON_TAKE_PROFIT", "true").lower() == "true"
ALERT_DAILY  = os.getenv("ALERT_DAILY_SUMMARY",  "true").lower() == "true"
ALERT_ERROR  = os.getenv("ALERT_ON_ERROR",        "true").lower() == "true"
PAPER        = os.getenv("PAPER_TRADING",         "false").lower() == "true"

_ROUND_TRIP_PCT = 0.10   # 0.05% open + 0.05% close fee estimate

# Dedup guard: suppress duplicate close notifications for the same coin within 60s.
# Needed because exit_manager, trading_engine sync, and category_engine can all
# detect the same close independently and call alert_trade_closed().
_DEDUP_PATH = Path("data/telegram_alert_dedup.json")
_dedup_lock = RLock()
try:
    _close_notified: dict[str, float] = {
        str(key): float(value)
        for key, value in json.loads(_DEDUP_PATH.read_text(encoding="utf-8")).items()
    }
except Exception:
    _close_notified = {}
_CLOSE_DEDUP_WINDOW = 60.0               # seconds


def _claim_alert(key: str, window_secs: float) -> bool:
    """Persist alert idempotency so reconnects/restarts do not duplicate events."""
    token = str(key or "").strip().upper()
    if not token:
        return True
    now = time.time()
    with _dedup_lock:
        previous = float(_close_notified.get(token, 0.0) or 0.0)
        if now - previous < max(float(window_secs), 0.0):
            try:
                from telegram_control_plane import control_plane
                control_plane.count("telegram_duplicate_alerts")
            except Exception:
                pass
            return False
        _close_notified[token] = now
        cutoff = now - 7 * 86400.0
        stale = [item for item, timestamp in _close_notified.items() if float(timestamp or 0.0) < cutoff]
        for item in stale:
            _close_notified.pop(item, None)
        try:
            _DEDUP_PATH.parent.mkdir(parents=True, exist_ok=True)
            tmp = _DEDUP_PATH.with_suffix(".json.tmp")
            tmp.write_text(json.dumps(_close_notified, indent=2, ensure_ascii=True), encoding="utf-8")
            tmp.replace(_DEDUP_PATH)
        except Exception:
            logger.debug("Telegram alert dedup persistence failed", exc_info=True)
        return True


def _fmt_money(value: float) -> str:
    amount = float(value or 0.0)
    magnitude = abs(amount)
    if magnitude >= 0.01:
        return f"${amount:+.4f}"
    if magnitude >= 0.0001:
        return f"${amount:+.6f}"
    return f"${amount:+.8f}"


def _calc_fee(size_usdt: float, leverage: int = 1) -> float:
    # size_usdt is already the NOTIONAL — fee is notional × rate (no extra leverage factor)
    return round(size_usdt * _ROUND_TRIP_PCT / 100, 6)


def _net_tp(size_usdt: float, tp_pct: float, leverage: int = 1) -> float:
    # size_usdt is NOTIONAL; PnL = notional × move_pct (no extra leverage factor)
    gross = size_usdt * (tp_pct / 100)
    return round(gross - _calc_fee(size_usdt, leverage), 6)


def _net_sl(size_usdt: float, sl_pct: float, leverage: int = 1) -> float:
    # size_usdt is NOTIONAL; PnL = notional × move_pct (no extra leverage factor)
    gross = size_usdt * (sl_pct / 100)
    return round(gross + _calc_fee(size_usdt, leverage), 6)


async def send_alert(text: str) -> bool:
    if not TOKEN or not CHAT_ID:
        return False
    first = str(text or "").splitlines()[0].upper() if text else ""
    if "LIVE" not in first and "PAPER" not in first:
        text = f"{'📄 PAPER' if PAPER else '🔴 LIVE'}\n\n{text}"
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.post(BASE, json={
                "chat_id":    CHAT_ID,
                "text":       text,
                "parse_mode": "Markdown",
            })
            response.raise_for_status()
        return True
    except Exception as e:
        logger.warning("Alert failed: %s — %r", type(e).__name__, str(e) or "(no message)")
        return False


async def alert_trade_executed(
    side, coin, size, price, stop, take, order_id,
    leverage: int = 5, tier_name: str = "", extra: str = "", explanation: str = "",
    tp_mode: str = "",
) -> None:
    if not ALERT_TRADE:
        return
    if order_id and not _claim_alert(f"ENTRY:{order_id}", 7 * 86400.0):
        return
    mode = "📝 PAPER" if PAPER else "⚡ LIVE"
    is_long = side in ("BUY", "buy", "LONG", "long")
    arrow = "🟢 LONG" if is_long else "🔴 SHORT"
    lev = max(int(leverage or 1), 1)
    notional = float(size or 0.0)
    margin = (notional / lev) if lev > 0 else notional
    try:
        sl_pct = abs(float(price) - float(stop)) / float(price) * 100 if float(price) > 0 else 0.0
    except Exception:
        sl_pct = 0.0
    tp_ref = ""
    try:
        if float(take or 0.0) > 0:
            tp_ref = f"  TP(ref):  `${float(take):,.6f}`\n"
    except Exception:
        tp_ref = ""

    engine_line = f"  Engine:  _{extra}_\n" if extra else ""
    oid = str(order_id or "").strip()
    oid_short = oid[-10:] if len(oid) > 10 else oid
    mode_norm = str(tp_mode or "").strip().lower()
    if mode_norm not in {"off", "reference_only", "exchange_tp", "hybrid"}:
        mode_norm = ""
    if PAPER:
        algo_line = "  Algo:    _(paper mode — no real orders)_"
    else:
        if mode_norm in {"exchange_tp", "hybrid"}:
            algo_line = "  Algo:    `SL ON` · `TP ON` (milestones+trail)"
        elif mode_norm == "reference_only":
            algo_line = "  Algo:    `SL ON` · `TP REF` (milestones+trail)"
        else:
            algo_line = "  Algo:    `SL ON` · `TP OFF` (milestones+trail)"
    note = ""
    if explanation:
        # Keep optional notes short; operator can inspect logs/telemetry for the full rationale.
        note = f"\n  Note:    `{str(explanation).strip()[:120]}`"

    await send_alert(
        f"{arrow} *{coin}* · {mode}\n"
        f"{engine_line}"
        f"  Entry:   `${float(price):,.6f}`\n"
        f"  Size:    `${notional:,.2f}` · `{lev}x` (m`${margin:,.2f}`)\n"
        f"  SL:      `${float(stop):,.6f}` (`-{sl_pct:.2f}%`)\n"
        f"{tp_ref}"
        f"  ID:      `{oid_short}`\n"
        f"{algo_line}"
        f"{note}"
    )


async def alert_trade_closed(
    engine: str,
    coin: str,
    direction: str,
    entry_price: float,
    exit_price: float,
    size_usdt: float,
    pnl_usdt: float,
    exit_reason: str,
    leverage: int = 5,
    via_exchange: bool = False,
    dedup_key: str | None = None,
    pnl_status: str | None = None,
) -> None:
    if not ALERT_TRADE:
        return
    dedup_token = str(dedup_key or coin or "").upper()
    window = 7 * 86400.0 if dedup_key else _CLOSE_DEDUP_WINDOW
    if not _claim_alert(f"CLOSE:{dedup_token}", window):
        logger.debug("alert_trade_closed dedup suppressed: %s", dedup_token)
        return
    mode = "📝 PAPER" if PAPER else "⚡ LIVE"
    arrow = "🟢 ↗ LONG" if direction.lower() == "long" else "🔴 ↘ SHORT"
    header = "🎯 Position Closed" if pnl_usdt >= 0 else "🛑 Position Closed"
    fee = _calc_fee(float(size_usdt), leverage)
    move_pct = 0.0
    if entry_price > 0:
        move_pct = (
            (float(exit_price) - float(entry_price)) / float(entry_price) * 100
            if direction.lower() == "long"
            else (float(entry_price) - float(exit_price)) / float(entry_price) * 100
        )
    channel = "exchange sync" if via_exchange else "bot close"
    net_est = round(float(pnl_usdt or 0.0) - fee, 6)
    status = str(pnl_status or ("PAPER_FINAL" if PAPER else "PROVISIONAL")).upper()
    await send_alert(
        f"{header} — *{coin}* | {mode}\n"
        f"  Engine:   _{engine}_\n"
        f"  Side:     {arrow}\n"
        f"  Entry:    `${float(entry_price):,.6f}`\n"
        f"  Exit:     `${float(exit_price):,.6f}`\n"
        f"  Move:     `{move_pct:+.2f}%` × `{leverage}x`\n"
        f"  Size:     `${float(size_usdt):,.2f}` notional\n"
        f"  Realized: `{_fmt_money(float(pnl_usdt or 0.0))}`\n"
        f"  Fee est:  `{_fmt_money(fee)}`\n"
        f"  Net est:  `{_fmt_money(net_est)}`\n"
        f"  PnL status: `{status}`\n"
        f"  Reason:   `{exit_reason}` ({channel})"
    )


async def alert_stop_loss(
    coin, entry, exit_price, pct, size, leverage: int = 5
) -> None:
    if not ALERT_SL:
        return
    net = _net_sl(float(size), float(pct), leverage)
    await send_alert(
        f"🛑 *STOP LOSS — {coin}*\n"
        f"  Entry:  `${float(entry):,.6f}`\n"
        f"  Exit:   `${float(exit_price):,.6f}`\n"
        f"  Loss:   `-{float(pct):.2f}%` × `{leverage}x` → `-${net:.4f}`"
    )


async def alert_take_profit(
    coin, entry, exit_price, pct, size, leverage: int = 5,
    realized_pnl_usdt: float | None = None,
    fee_est_usdt: float | None = None,
    label: str = "PARTIAL CLOSE",
) -> None:
    if not ALERT_TP:
        return
    fee = float(fee_est_usdt) if fee_est_usdt is not None else _calc_fee(float(size), leverage)
    realized = float(realized_pnl_usdt) if realized_pnl_usdt is not None else (_net_tp(float(size), float(pct), leverage) + fee)
    net = round(realized - fee, 6)
    await send_alert(
        f"🎯 *TAKE PROFIT — {coin}* ({label})\n"
        f"  Entry:  `${float(entry):,.6f}`\n"
        f"  Exit:   `${float(exit_price):,.6f}`\n"
        f"  Move:   `+{float(pct):.2f}%` × `{leverage}x`\n"
        f"  Realized: `{_fmt_money(realized)}`\n"
        f"  Fee est:  `{_fmt_money(fee)}`\n"
        f"  Net est:  `{_fmt_money(net)}` ✅"
    )


async def alert_position_closed_by_exchange(
    coin: str, entry: float, exit_price: float,
    pnl_usdt: float, reason: str, leverage: int = 5,
) -> None:
    """Alert for exchange-triggered TP/SL algo close."""
    emoji = "🎯" if pnl_usdt >= 0 else "🛑"
    label = "TAKE PROFIT" if pnl_usdt >= 0 else "STOP LOSS"
    pct   = abs((exit_price - entry) / entry * 100) if entry > 0 else 0.0
    await send_alert(
        f"{emoji} *{label} — {coin}* _(exchange algo)_\n"
        f"  Entry:  `${entry:,.6f}`\n"
        f"  Exit:   `${exit_price:,.6f}`\n"
        f"  Move:   `{pct:.2f}%` × `{leverage}x`\n"
        f"  PnL:    `${pnl_usdt:+.4f}`"
    )


async def alert_daily_summary(
    total,
    wins,
    losses,
    pnl,
    balance,
    pct,
    fee_usdt: float | None = None,
    gross_pnl: float | None = None,
    open_positions: int | None = None,
    exit_counts: dict | None = None,
    engine_stats: dict | None = None,
    report_date: str | None = None,
    timezone_label: str = "UTC",
) -> None:
    if not ALERT_DAILY:
        return
    emoji    = "📈" if pnl >= 0 else "📉"
    win_rate = round(wins / total * 100) if total else 0
    title_date = f" - {report_date} {timezone_label}" if report_date else ""
    lines = [
        f"{emoji} *Daily Summary{title_date}*",
        f"  Trades:  `{total}` | ✅ `{wins}W` ❌ `{losses}L` | WR: `{win_rate}%`",
        f"  Net P&L: `${pnl:+.4f}` (`{pct:+.2f}%`)",
    ]
    if gross_pnl is not None or fee_usdt is not None:
        lines.append(f"  Gross/Fee: `${float(gross_pnl or 0.0):+.4f}` / `${float(fee_usdt or 0.0):+.6f}`")
    if open_positions is not None:
        lines.append(f"  Open Positions: `{int(open_positions)}`")
    if exit_counts:
        early = sum(int(v or 0) for k, v in exit_counts.items() if "early_invalidation" in str(k))
        tp_hits = sum(int(v or 0) for k, v in exit_counts.items() if "tp" in str(k).lower())
        lines.append(f"  Exit Counts: EI `{early}` | TP `{tp_hits}`")
    if engine_stats:
        ranked = sorted(engine_stats.items(), key=lambda item: float((item[1] or {}).get("net_pnl_usdt", 0.0) or 0.0), reverse=True)[:3]
        if ranked:
            engine_line = ", ".join(f"{name} ${float(data.get('net_pnl_usdt', 0.0)):+.2f}" for name, data in ranked)
            lines.append(f"  Engines: `{engine_line}`")
    lines.append(f"  Balance: `${balance:,.2f} USDT`")
    await send_alert("\n".join(lines))


async def alert_daily_loss_halt(loss_pct, limit_pct) -> None:
    await send_alert(
        f"🚨 *TRADING HALTED*\n"
        f"  Daily loss limit hit!\n"
        f"  Loss: `{loss_pct:.1f}%` / Limit: `{limit_pct:.1f}%`"
    )


async def alert_error(source, message) -> None:
    if not ALERT_ERROR:
        return
    await send_alert(f"⚠️ *Error — {source}*\n`{str(message)[:300]}`")


async def alert_oi_spike(coin: str, change_pct: float, oi_usd: float, context: str = "") -> None:
    arrow = "📈" if change_pct > 0 else "📉"
    await send_alert(
        f"{arrow} *OI Spike — {coin}*\n"
        f"  Change: `{change_pct:+.1f}%`\n"
        f"  OI:     `${oi_usd/1e6:.2f}M`\n"
        f"  _Major move likely imminent_"
    )


async def alert_engine_event(engine: str, emoji: str, event: str) -> None:
    await send_alert(f"{emoji} *{engine}* — {event}")


async def alert_winrate_drop(
    global_wr: float,
    threshold: float,
    trades_sampled: int,
    worst_engine: str | None = None,
    worst_engine_wr: float = 0.0,
) -> None:
    """Alert when rolling 20-trade win rate drops below the configured threshold."""
    worst_line = (
        f"\n  📉 Worst engine: *{worst_engine}* `{worst_engine_wr:.1f}%`"
        if worst_engine else ""
    )
    await send_alert(
        f"⚠️ *Win Rate Alert*\n\n"
        f"  Rolling-20 WR dropped to `{global_wr:.1f}%`\n"
        f"  Threshold: `{threshold:.0f}%` | Sample: `{trades_sampled}` trades"
        f"{worst_line}\n\n"
        f"  Consider reviewing entries or running `/winrate` for detail.\n"
        f"  _Position size auto-reduced if consecutive losses hit limit._"
    )


async def alert_winrate_recovery(global_wr: float, threshold: float) -> None:
    """Alert when rolling win rate recovers above the threshold."""
    await send_alert(
        f"✅ *Win Rate Recovered*\n\n"
        f"  Rolling-20 WR back to `{global_wr:.1f}%` "
        f"(threshold: `{threshold:.0f}%`)\n"
        f"  Full position sizing restored."
    )

"""
OxyPulse — bot.py  V6.1
Premium OKX-style Telegram control center. 26 engines, V6.1 capital-adaptive
intelligence, drawdown guard, exposure control, smart execution.

Panel hierarchy:
  Home → Portfolio · Trades · Intelligence · Risk · Control · Settings · Help
"""
from __future__ import annotations
import os, re, logging, logging.handlers, asyncio, time, atexit, sys, math
from contextvars import ContextVar
from functools import wraps
from pathlib import Path
from typing import Dict
from datetime import datetime, timezone, time as dtime
from zoneinfo import ZoneInfo

from runtime_ownership import (
    RUNTIME_OWNERSHIP_EXIT_CODE,
    RuntimeOwnership,
    RuntimeOwnershipDenied,
)

_runtime_ownership = RuntimeOwnership()
if __name__ == "__main__":
    try:
        _runtime_ownership.acquire()
    except RuntimeOwnershipDenied as exc:
        print(str(exc), file=sys.stderr, flush=True)
        raise SystemExit(RUNTIME_OWNERSHIP_EXIT_CODE)
    atexit.register(_runtime_ownership.release)

from dotenv import load_dotenv
load_dotenv(dotenv_path=str(Path(__file__).resolve().parent / ".env"), override=True)

from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.error import NetworkError, RetryAfter, TimedOut
from telegram.ext import (
    Application, ApplicationBuilder, CommandHandler,
    CallbackQueryHandler, MessageHandler, ContextTypes, filters,
)
from okx_client import OKXClient, refresh_universe
from coin_universe import get_futures_universe
from trading_engine import engine
from category_tracker import (
    format_engine_report, format_combined_report,
    get_engine_stats, get_combined_stats, ENGINE_NAMES, ENGINE_EMOJI,
    get_engine_recent_performance,
    format_winrate_report, get_winrate_alert_status, engine_display_name,
)
from position_registry import get_all as reg_get_all
from strategy_categories import STRATEGY_DESCRIPTIONS, killzone_status
import market_mode as mm
from notifier import send_alert, alert_winrate_drop, alert_winrate_recovery
import settings_store as _settings
import performance_tracker as _perf
import regime_detector as _regime
import session_context as _sess
import liq_heatmap as _liq
from entry_filter import clear_blackswan
from market_data_stream import latest_price
import tg_panels as _tp
import live_position_truth as _pnl_truth
import telegram_backend as _tg_backend
from telegram_control_plane import control_plane as _tg_control_plane

Path("logs").mkdir(exist_ok=True)
_fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")
_fh  = logging.handlers.RotatingFileHandler("logs/oxypulse.log", maxBytes=5_000_000, backupCount=3, encoding="utf-8")
_fh.setFormatter(_fmt); _ch = logging.StreamHandler(); _ch.setFormatter(_fmt)
logging.basicConfig(level=logging.INFO, handlers=[_fh, _ch])
# Suppress noisy per-request httpx logs — these flood the log file and can fill disk
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("telegram.vendor.ptb_urllib3").setLevel(logging.WARNING)
logger = logging.getLogger("OxyPulse")

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN") or os.getenv("TELEGRAM_BOT_TOKEN")
OWNER_ID       = os.getenv("OWNER_TELEGRAM_ID", "")
ALLOWED_USERS  = os.getenv("ALLOWED_USERS", OWNER_ID)
MAX_HISTORY    = int(os.getenv("MAX_HISTORY", 20))
PAPER_TRADING  = os.getenv("PAPER_TRADING", "false").lower() == "true"

allowed_set = {uid.strip() for uid in ALLOWED_USERS.split(",") if uid.strip()}
histories: Dict[int, list] = {}
chat_mode: Dict[int, str]  = {}
okx = OKXClient()
_tg_request_token: ContextVar[str | None] = ContextVar("telegram_request_token", default=None)
_runtime_startup_task: asyncio.Task | None = None

def is_allowed(uid: int) -> bool: return not allowed_set or str(uid) in allowed_set
async def _deny(update: Update) -> None:
    if update.message: await update.message.reply_text("⛔ Not authorised.")
    elif update.callback_query: await update.callback_query.answer("⛔ Unauthorised", show_alert=True)
def _paper() -> str: return "📄 PAPER" if PAPER_TRADING else "🔴 LIVE"
def _v(key, default=None): return _settings.get_val(key, default)
def _tog(key, on, off): return on if _v(key) else off


def _with_mode_banner(text: str) -> str:
    raw = str(text or "")
    first = raw.splitlines()[0].upper() if raw else ""
    if any(marker in first for marker in ("LIVE ARMED", "LIVE UNARMED", "LIVE HALTED", "LIVE EMERGENCY", "PAPER")):
        return raw
    if PAPER_TRADING:
        badge = "📄 PAPER"
    else:
        try:
            from live_state import state as _live_state
            badge = _live_state_badge(_live_state.snapshot())
        except Exception:
            badge = "🔴 LIVE UNKNOWN"
    return f"*{badge}*\n\n{raw}"


async def _tracked_reply(message, text: str, **kwargs):
    """Send a Telegram reply while preserving backend/send latency boundaries."""
    token = _tg_request_token.get()
    if token:
        _tg_control_plane.backend_complete(token)
    try:
        result = await message.reply_text(_with_mode_banner(text), **kwargs)
    except Exception as exc:
        if token:
            _tg_control_plane.error(token, exc, api_failure=True)
        raise
    if token:
        _tg_control_plane.sent(token)
    return result


def _instrument_command(name: str, fn):
    @wraps(fn)
    async def _wrapped(update, context):
        token = _tg_control_plane.begin("command", name, update_id=getattr(update, "update_id", None))
        _tg_control_plane.dispatch(token)
        marker = _tg_request_token.set(token)
        try:
            result = await fn(update, context)
            if _tg_control_plane.is_pending(token):
                _tg_control_plane.backend_complete(token)
                _tg_control_plane.sent(token)
            return result
        except Exception as exc:
            if _tg_control_plane.is_pending(token):
                _tg_control_plane.error(token, exc, api_failure=isinstance(exc, (NetworkError, TimedOut)))
            raise
        finally:
            _tg_request_token.reset(marker)
    return _wrapped


def _instrument_callback(fn):
    @wraps(fn)
    async def _wrapped(update, context):
        query = getattr(update, "callback_query", None)
        action = str(getattr(query, "data", "unknown") or "unknown")
        token = _tg_control_plane.begin("button", action, update_id=getattr(update, "update_id", None))
        _tg_control_plane.dispatch(token)
        marker = _tg_request_token.set(token)
        try:
            result = await fn(update, context)
            if _tg_control_plane.is_pending(token):
                _tg_control_plane.backend_complete(token)
                _tg_control_plane.sent(token)
            return result
        except Exception as exc:
            if _tg_control_plane.is_pending(token):
                _tg_control_plane.error(token, exc, api_failure=isinstance(exc, (NetworkError, TimedOut)))
            raise
        finally:
            _tg_request_token.reset(marker)
    return _wrapped


def _enabled_engine_count() -> int:
    return sum(
        1 for name in ENGINE_NAMES
        if _settings.get_engine_mode(name) in ("live", "probation", "shadow")
    )


def _engine_mode_breakdown() -> dict[str, int]:
    counts = {
        "live": 0,
        "shadow": 0,
        "disabled": 0,
        "probation": 0,
        "total": len(ENGINE_NAMES),
    }
    for name in ENGINE_NAMES:
        mode = _settings.get_engine_mode(name)
        if mode == "shadow":
            counts["shadow"] += 1
        elif mode == "disabled":
            counts["disabled"] += 1
        elif mode == "probation":
            counts["probation"] += 1
            counts["live"] += 1
        else:
            counts["live"] += 1
    return counts


def _engine_breakdown_text() -> str:
    counts = _engine_mode_breakdown()
    return (
        f"{counts['live']} LIVE · {counts['shadow']} SHADOW · "
        f"{counts['disabled']} DISABLED · {counts['total']} TOTAL"
    )


def _startup_ready_text(balance: float = 0.0, sync_status: str = "Boot sync active") -> str:
    try:
        from capital_manager import get_profile as _cm_get_profile
        profile = _cm_get_profile(balance)
        profile_name = str(profile.mode or "MICRO").upper()
    except Exception:
        profile_name = "MICRO"

    lev = int(_v("leverage_default", 10))
    risk_pct = float(_v("risk_pct", 1.25))
    max_pos = int(_v("max_open_positions", 3))
    open_risk = float(_v("rule_357_max_open_risk_pct", 25.0))
    min_vote = float(_v("min_vote_score", 3.0))
    if PAPER_TRADING:
        live_status = "PAPER READY"
        status_icon = "🟡"
    else:
        try:
            from live_state import state as _startup_live_state
            _live_snapshot = _startup_live_state.snapshot()
            _is_armed = bool(_live_snapshot.get("armed")) and str(_live_snapshot.get("state")) == "LIVE_ARMED"
        except Exception:
            _is_armed = False
        live_status = "LIVE ARMED" if _is_armed else "LIVE READY UNARMED"
        status_icon = "🔴" if _is_armed else "🟢"
    scanner_status = "Active" if engine.running else "Idle"

    return (
        "⚡ *OxyPulse V6.1*\n"
        "━━━━━━━━━━━━━━━━━━\n"
        f"{status_icon} Status: `{live_status}`\n"
        f"🧠 Engines: `{_engine_breakdown_text()}`\n"
        f"⚙️ Profile: `{profile_name}`\n"
        f"📈 Leverage: `{lev}x` target\n"
        f"💸 Risk/Trade: `{risk_pct:.2f}%`\n"
        f"📦 Max Open: `{max_pos}`\n"
        f"🛡️ Open-Risk Cap: `{open_risk:.1f}%`\n"
        f"🎯 Min Votes: `{min_vote:.1f} / 3`\n"
        f"🔄 Sync: `{sync_status}`\n"
        "🧷 Exit Guard: `Active`\n"
        f"📡 Scanner: `{scanner_status}`\n\n"
        f"Scanner is {'running' if engine.running else 'stopped'}\n"
        "Use /sync for reconciliation\n"
        "Use /v6 and the Risk panel for exposure and lane usage"
    )


def _engine_target_leverage(name: str | None = None) -> int:
    low = int(_v("leverage_min", 5))
    high = int(_v("leverage_max", 15))
    if name:
        lev = int(_v(f"{name.lower()}_leverage", _v("leverage_default", low)))
    else:
        lev = int(_v("leverage_default", low))
    return max(low, min(lev, high))


def _settings_text() -> str:
    lev = int(_v("leverage_default", 10))
    lev_min = int(_v("leverage_min", 5))
    lev_max = int(_v("leverage_max", 15))
    risk_pct = float(_v("risk_pct", 1.25))
    cooldown = int(_v("cooldown_mins", 15))
    max_pos = int(_v("max_open_positions", 10))
    dir_cap = int(_v("max_same_direction_positions", 4))
    open_risk = float(_v("rule_357_max_open_risk_pct", 8.0))
    size_floor = float(_v("position_size_usdt", 100.0))
    size_cap = float(_v("position_size_cap_usdt", 250.0))
    daily_cap = float(_v("daily_loss_pct", 12.0))
    weekly_cap = float(_v("weekly_loss_pct", 25.0))
    margin_est = size_floor / lev if lev else size_floor
    return (
        "⚙️ *Settings*\n\n"
        f"  Trade size        `${size_floor:.0f}` notional (`~${margin_est:.0f}` margin at {lev}x)\n"
        f"  Daily loss cap    `{daily_cap:.0f}%`\n"
        f"  Weekly loss cap   `{weekly_cap:.0f}%`\n"
        f"  Target leverage   `{lev}x`  _(range {lev_min}-{lev_max}x)_\n"
        f"  Risk per trade    `{risk_pct}%`\n"
        f"  Cooldown          `{cooldown} min`\n"
        f"  Max open trades   `{max_pos}`  |  Same-side: `{dir_cap}`\n"
        f"  Open-risk cap     `{open_risk}%`  |  Size cap: `${size_cap:.0f}`\n\n"
        "_Use the controls below to tune the live defaults._"
    )


def _render_ai_scan(title: str, scored: list[dict], top_n: int = 5, include_confidence: bool = False) -> str:
    longs = sorted([s for s in scored if s["signal"] == "LONG"], key=lambda x: x["composite"], reverse=True)
    shorts = sorted([s for s in scored if s["signal"] == "SHORT"], key=lambda x: x["composite"])
    lines = [title, f"_{killzone_status()}_", ""]
    if longs:
        lines.append(f"🟢 *LONG ({len(longs)})*")
        for s in longs[:top_n]:
            icon = "🔥" if s["level"] == "HIGH" else "⚡" if s["level"] == "MEDIUM" else "·"
            conf = f" `{int(s['confidence'] * 100)}%`" if include_confidence and "confidence" in s else ""
            lines.append(f"  {icon} `{s['coin']:8}` `{s['composite']:+.3f}`{conf}")
    if shorts:
        if longs:
            lines.append("")
        lines.append(f"🔴 *SHORT ({len(shorts)})*")
        for s in shorts[:top_n]:
            icon = "🔥" if s["level"] == "HIGH" else "⚡" if s["level"] == "MEDIUM" else "·"
            conf = f" `{int(s['confidence'] * 100)}%`" if include_confidence and "confidence" in s else ""
            lines.append(f"  {icon} `{s['coin']:8}` `{s['composite']:+.3f}`{conf}")
    if not longs and not shorts:
        lines.append("⚪ _No strong signals._")
    hold_count = len([s for s in scored if s["signal"] == "HOLD"])
    lines.append("")
    lines.append(f"⚪ HOLD: `{hold_count}`")
    return "\n".join(lines)

# ── Keyboards ─────────────────────────────────────────────────────────────────

def main_menu() -> InlineKeyboardMarkup:
    """V6.1 premium home menu with engine toggle."""
    return _tp.kb_home(engine.running)


def _proposal_kb() -> InlineKeyboardMarkup:
    """Legacy proposal keyboard removed with the AI flow."""
    return main_menu()


def engines_menu() -> InlineKeyboardMarkup:
    rows = []
    for name in ENGINE_NAMES:
        eng2  = engine.engines[name]
        emoji = ENGINE_EMOJI[name]
        mode = _settings.get_engine_mode(name)
        mode_dot = {"live": "●", "probation": "⚠", "shadow": "👻", "disabled": "✕"}.get(mode, "✕")
        pos   = eng2.trade_count()
        pos_s = f"  [{pos}]" if pos else ""
        rows.append([InlineKeyboardButton(
            f"{mode_dot} {emoji} {engine_display_name(name)}{pos_s}",
            callback_data=f"engine_detail_{name}",
        )])
    rows.append([
        InlineKeyboardButton(
            "⏸ Stop All" if engine.running else "▶️ Start All",
            callback_data="eng_stop_all" if engine.running else "eng_start_all",
        ),
        InlineKeyboardButton("🛑 Close All", callback_data="close_all_confirm"),
    ])
    rows.append([InlineKeyboardButton("🏠 Home", callback_data="main_menu")])
    return InlineKeyboardMarkup(rows)


def engine_detail_kb(name: str) -> InlineKeyboardMarkup:
    eng2    = engine.engines.get(name.upper())
    running = eng2.running if eng2 else False
    mode = _settings.get_engine_mode(name.upper())
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(
             "⏸ Stop" if running else "▶️ Start",
             callback_data=f"eng_toggle_{name}"),
         InlineKeyboardButton(
             f"🎛 Mode: {mode.upper()}",
             callback_data=f"eng_mode_cycle_{name}")],
        [InlineKeyboardButton(
             f"🛑 Close {engine_display_name(name)} positions",
             callback_data=f"eng_closeall_{name}")],
        [InlineKeyboardButton("⚙️ Engines", callback_data="engines_menu"),
         InlineKeyboardButton("🏠 Home",    callback_data="main_menu")],
    ])


def positions_kb(positions: dict, live_snapshot: dict | None = None) -> InlineKeyboardMarkup:
    rows = []
    for coin, t in list(positions.items())[:8]:
        en    = t.get("engine", "?")
        side  = "↗" if t.get("side") == "buy" else "↘"
        row = ((live_snapshot or {}).get("positions", {}) or {}).get(str(coin).upper(), {})
        pnl = row.get("unrealised_pnl")
        src = row.get("source_label", "🔴 STALE")
        pnl_s = f"{src} {float(pnl):+.2f}" if pnl is not None else f"{src} —"
        rows.append([InlineKeyboardButton(
            f"{side} {coin}  [{en}]  {pnl_s}  ✕",
            callback_data=f"close_coin_{coin}",
        )])
    rows.append([
        InlineKeyboardButton("🛑 Close All", callback_data="close_all_confirm"),
        InlineKeyboardButton("🔄 Refresh",   callback_data="positions_view"),
        InlineKeyboardButton("🏠 Home",      callback_data="main_menu"),
    ])
    return InlineKeyboardMarkup(rows)


def reports_menu() -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton("📊 Combined", callback_data="report_combined"),
         InlineKeyboardButton("📈 Win Rate", callback_data="report_winrate")],
        [InlineKeyboardButton("🧾 Recent Trades", callback_data="report_trades"),
         InlineKeyboardButton("💰 Portfolio", callback_data="portfolio")],
    ]
    names = ENGINE_NAMES[:]
    for i in range(0, len(names), 3):
        rows.append([
            InlineKeyboardButton(f"{ENGINE_EMOJI[n]} {engine_display_name(n)}", callback_data=f"report_{n}")
            for n in names[i:i + 3]
        ])
    rows.append([InlineKeyboardButton("🏠 Home", callback_data="main_menu")])
    return InlineKeyboardMarkup(rows)


def settings_menu() -> InlineKeyboardMarkup:
    """V6.1 settings hub — sections via tg_panels."""
    return _tp.kb_settings_hub(_settings)


async def _strategy_panel_payload() -> tuple[str, InlineKeyboardMarkup]:
    try:
        bal = float(await okx.get_usdt_balance())
    except Exception:
        bal = float(getattr(engine, "_last_balance_usdt", 0.0) or 0.0)
    return (
        _tp.panel_settings_strategy_text(_settings, balance=bal),
        _tp.kb_settings_strategy(_settings, balance=bal),
    )


async def _settings_dashboard_payload() -> tuple[str, InlineKeyboardMarkup]:
    try:
        bal = float(await okx.get_usdt_balance())
    except Exception:
        bal = float(getattr(engine, "_last_balance_usdt", 0.0) or 0.0)

    exposure = None
    try:
        from exposure_tracker import get_exposure as _gexp
        exposure = _gexp(bal)
    except Exception:
        exposure = None

    try:
        regime = _regime.get_regime()
    except Exception:
        regime = "—"
    try:
        session = killzone_status()
    except Exception:
        session = "—"

    if engine.running or getattr(engine, "_last_sync_ts", 0.0):
        try:
            sync_status = engine.get_sync_health_label()
        except Exception:
            sync_status = "🟢 OK" if engine.running else "Standby"
    else:
        sync_status = "Boot sync pending"
    status_label = "PAPER LIVE" if PAPER_TRADING and engine.running else ("LIVE" if engine.running else "STOPPED")
    scanner_status = "Active" if engine.running else "Idle"
    open_positions = len(engine.all_open_positions())
    live_snapshot = await _live_position_snapshot()

    return (
        _tp.panel_settings_dashboard_text(
            _settings,
            balance=bal,
            exposure=exposure,
            running=engine.running,
            status_label=status_label,
            regime=regime,
            session=session,
            sync_status=sync_status,
            scanner_status=scanner_status,
            open_positions=open_positions,
            live_snapshot=live_snapshot,
        ),
        settings_menu(),
    )


async def _risk_panel_payload() -> tuple[str, InlineKeyboardMarkup]:
    try:
        bal = float(await okx.get_usdt_balance())
    except Exception:
        bal = float(getattr(engine, "_last_balance_usdt", 0.0) or 0.0)
    exposure = None
    try:
        from exposure_tracker import get_exposure as _gexp
        exposure = _gexp(bal)
    except Exception:
        exposure = None
    return (
        _tp.panel_settings_risk_text(_settings, balance=bal, exposure=exposure),
        _tp.kb_settings_risk(_settings, balance=bal),
    )


async def _modes_panel_payload() -> tuple[str, InlineKeyboardMarkup]:
    snapshot = {}
    try:
        snapshot = await okx.get_account_snapshot()
    except Exception:
        snapshot = {}
    bal = float(
        snapshot.get("avail_eq_usdt", 0.0)
        or getattr(engine, "_last_balance_usdt", 0.0)
        or 0.0
    )
    available_margin = float(
        snapshot.get("usdt_avail_bal", 0.0)
        or snapshot.get("avail_eq_usdt", 0.0)
        or bal
    )
    exposure = None
    try:
        from exposure_tracker import get_exposure as _gexp
        exposure = _gexp(bal)
    except Exception:
        exposure = None
    return (
        _tp.panel_settings_modes_text(
            _settings,
            balance=bal,
            exposure=exposure,
            available_margin=available_margin,
        ),
        _tp.kb_settings_modes(_settings, balance=bal),
    )


async def _capital_panel_payload() -> tuple[str, InlineKeyboardMarkup]:
    return await _modes_panel_payload()


async def _settings_balance() -> float:
    try:
        return float(await okx.get_usdt_balance())
    except Exception:
        return float(getattr(engine, "_last_balance_usdt", 0.0) or 0.0)


async def _active_risk_keys() -> tuple[str, str, str]:
    bal = await _settings_balance()
    caps = _tp._active_risk_caps(_settings, balance=bal)
    mode = str(caps.get("mode") or "MICRO").upper()
    return (
        f"v6_{mode.lower()}_max_exposure_pct",
        f"portfolio_same_direction_pct_{mode.lower()}",
        mode,
    )


def _bounded_partial_pct(primary_key: str, other_key: str, delta: float) -> float:
    current = float(_v(primary_key, 30.0))
    other = float(_v(other_key, 20.0))
    max_allowed = max(10.0, min(60.0, 90.0 - other))
    return round(max(10.0, min(current + delta, max_allowed)), 1)


def _stepped_min_volume(current: float, direction: int) -> float:
    value = max(float(current or 0.0), 0.0)
    if value >= 2_000_000.0:
        step = 250_000.0
    elif value >= 1_000_000.0:
        step = 100_000.0
    elif value >= 250_000.0:
        step = 50_000.0
    else:
        step = 25_000.0
    if direction >= 0:
        return min(value + step, 10_000_000.0)
    return max(value - step, 0.0)


def _apply_deployment_mode_choice(next_mode: str) -> None:
    current_mode = str(_settings.get_val("deployment_mode", "Guarded") or "Guarded")
    try:
        from capital_manager import (
            DEPLOYMENT_MODE_CHOICES,
            DEPLOYMENT_GUARDED,
            DEPLOYMENT_BALANCED,
            DEPLOYMENT_MAX_DEPLOY,
            DEPLOYMENT_FULL_DEPLOYMENT,
            DEPLOYMENT_SINGLE_SHOT,
            DEPLOYMENT_SWARM,
            DEPLOYMENT_RECOVERY,
        )
        _ = list(DEPLOYMENT_MODE_CHOICES)
        presets = {
            DEPLOYMENT_GUARDED: {"deployment_respect_exposure_cap": True, "deployment_respect_headroom": True},
            DEPLOYMENT_BALANCED: {"deployment_respect_exposure_cap": True, "deployment_respect_headroom": True},
            DEPLOYMENT_MAX_DEPLOY: {"deployment_respect_exposure_cap": True, "deployment_respect_headroom": True},
            DEPLOYMENT_FULL_DEPLOYMENT: {"deployment_respect_exposure_cap": True, "deployment_respect_headroom": True},
            DEPLOYMENT_SINGLE_SHOT: {"deployment_respect_exposure_cap": True, "deployment_respect_headroom": True},
            DEPLOYMENT_SWARM: {"deployment_respect_exposure_cap": True, "deployment_respect_headroom": True},
            DEPLOYMENT_RECOVERY: {"deployment_respect_exposure_cap": True, "deployment_respect_headroom": True},
        }
    except Exception:
        presets = {
            "Guarded": {"deployment_respect_exposure_cap": True, "deployment_respect_headroom": True},
            "Balanced": {"deployment_respect_exposure_cap": True, "deployment_respect_headroom": True},
            "Max Deploy": {"deployment_respect_exposure_cap": True, "deployment_respect_headroom": True},
            "Full Deployment": {"deployment_respect_exposure_cap": True, "deployment_respect_headroom": True},
            "Single Shot": {"deployment_respect_exposure_cap": True, "deployment_respect_headroom": True},
            "Swarm": {"deployment_respect_exposure_cap": True, "deployment_respect_headroom": True},
            "Recovery": {"deployment_respect_exposure_cap": True, "deployment_respect_headroom": True},
        }

    _settings.set_val("deployment_mode", next_mode)
    try:
        from capital_scheduler import scheduler as _capital_scheduler
        _capital_scheduler.notify("deployment_mode_changed")
    except Exception:
        pass
    _settings.set_val("deployment_target_utilization_pct", 0.0)
    _settings.set_val("deployment_max_positions", 0)
    _settings.set_val("deployment_same_side_limit_pct", 0.0)
    _settings.set_val("deployment_total_exposure_limit_pct", 0.0)
    for key, value in presets.get(next_mode, {}).items():
        _settings.set_val(key, value)
    if current_mode != next_mode:
        logger.info("MODE APPLY: deployment %s -> %s", current_mode, next_mode)


def _apply_exposure_control_mode(next_mode: str) -> None:
    current_mode = str(_settings.get_val("exposure_control_mode", "Strict") or "Strict")
    _settings.set_val("exposure_control_mode", str(next_mode))
    if current_mode != str(next_mode):
        logger.info("MODE APPLY: exposure %s -> %s", current_mode, next_mode)


def engines_settings_kb() -> InlineKeyboardMarkup:
    from category_tracker import ENGINE_STYLE
    rows = []
    mode_icons = {"live": "●", "probation": "⚠", "shadow": "👻", "disabled": "✕"}
    for name in ENGINE_NAMES:
        mode = _settings.get_engine_mode(name)
        icon = mode_icons.get(mode, "✕")
        rows.append([InlineKeyboardButton(
            f"{icon} {ENGINE_EMOJI[name]} {engine_display_name(name)} [{mode[:4].upper()}] — {ENGINE_STYLE.get(name, '')}",
            callback_data=f"stg_eng_tog_{name}",
        )])
    rows.append([InlineKeyboardButton("← Settings", callback_data="settings_menu")])
    return InlineKeyboardMarkup(rows)


def help_menu() -> InlineKeyboardMarkup:
    """V6.1 help hub."""
    return _tp.kb_help()


# ── Text formatters ───────────────────────────────────────────────────────────

async def _live_position_snapshot(force_refresh: bool = False) -> dict:
    try:
        from position_registry import get_all_details as _reg_details
        registry = _reg_details()
    except Exception:
        registry = {}
    try:
        return await _pnl_truth.get_live_position_snapshot(
            okx,
            registry=registry,
            settings=_settings,
            force_refresh=force_refresh,
        )
    except Exception as exc:
        logger.warning("live position snapshot failed: %s", exc)
        return {
            "ok": False,
            "reason": str(exc),
            "source": "STALE",
            "source_label": "🔴 STALE",
            "confidence": "LOW",
            "open_pnl_usdt": None,
            "positions": {},
            "age_sec": 0.0,
            "stale_count": len(registry),
            "mismatch_count": 0,
            "local_active_count": 0,
            "local_unknown_count": len(registry),
            "local_unknown_symbols": sorted(registry),
        }


def _home_text(bal: float = 0.0, live_snapshot: dict | None = None) -> str:
    """V6.1 premium home dashboard text."""
    try: regime = _regime.get_regime()
    except Exception: regime = "—"
    try: session = killzone_status()
    except Exception: session = "—"

    capital_profile = dd_state = exposure = None
    v6_on = bool(_v("v6_enabled", False))
    try:
        if _v("v6_capital_adaptive_enabled", True):
            from capital_manager import get_profile as _cm
            capital_profile = _cm(bal)
        if _v("v6_anti_drawdown_enabled", False):
            from drawdown_guard import get_drawdown_state as _dds
            capital_profile_ref = capital_profile
            dd_state = _dds(capital_profile=capital_profile_ref)
        if _v("v6_correlation_control_enabled", True):
            from exposure_tracker import get_exposure as _gexp
            exposure = _gexp(bal)
    except Exception:
        pass

    return _tp.panel_home_text(
        bal=bal,
        combined_stats=get_combined_stats(),
        regime=regime,
        session=session,
        running_count=engine.running_count(),
        enabled_count=_enabled_engine_count(),
        open_positions=engine.total_positions(),
        drawdown_active=getattr(engine, "drawdown_active", False),
        paper=PAPER_TRADING,
        capital_profile=capital_profile,
        dd_state=dd_state,
        exposure=exposure,
        v6_on=v6_on,
        live_snapshot=live_snapshot,
    )


def _live_state_badge(snapshot: dict) -> str:
    state = str(snapshot.get("state", "UNKNOWN") or "UNKNOWN")
    if state == "LIVE_ARMED" and snapshot.get("armed"):
        return "🟢 LIVE ARMED"
    if state == "LIVE_READY_UNARMED":
        return "🟡 LIVE UNARMED"
    if state == "LIVE_HALTED":
        return "⛔ LIVE HALTED"
    if state == "LIVE_EMERGENCY_STOP":
        return "🚨 LIVE EMERGENCY"
    if state == "LIVE_PREFLIGHT_FAILED":
        return "🔴 PREFLIGHT FAILED"
    return f"🔴 {state.replace('_', ' ')}"


def _optional_money(value, *, signed: bool = False, decimals: int = 4) -> str:
    if value is None:
        return "Unavailable"
    number = float(value or 0.0)
    return f"${number:+,.{decimals}f}" if signed else f"${number:,.{decimals}f}"


def _production_status_text(runtime: dict, account: dict, positions: dict, pnl: dict, health: dict) -> str:
    live = runtime.get("live_state", {}) or {}
    queue = runtime.get("opportunities", {}) or {}
    orders = runtime.get("orders", {}) or {}
    scan = runtime.get("scan", {}) or {}
    preflight = runtime.get("preflight", {}) or {}
    preflight_checks = dict(preflight.get("checks", {}) or {})
    preflight_passed = sum(1 for row in preflight_checks.values() if bool((row or {}).get("ok")))
    preflight_total = len(preflight_checks)
    active_scan = bool(scan.get("started_at") and not scan.get("finished_at"))
    first_cycle = int(scan.get("cycle_number", 1) or 1) == 1
    warmup_label = " · STARTUP WARM-UP" if active_scan and first_cycle else " · SCANNING" if active_scan else ""
    open_positions = len((positions.get("positions", {}) or {}))
    final_label = "LIVE FINAL" if int(pnl.get("reconciling", 0) or 0) == 0 else "LIVE RECONCILING"
    age = positions.get("age_sec")
    age_text = "unknown" if age is None else f"{float(age):.1f}s"
    source = positions.get("source", positions.get("source_label", "UNKNOWN"))
    if positions.get("stale") and source not in {"UNAVAILABLE", "CACHED_OKX"}:
        source = f"CACHED {source}"
    return "\n".join([
        f"*{_live_state_badge(live)}{warmup_label}*",
        "",
        f"Preflight: `{'PASS' if preflight.get('ok') else 'FAILED'} {preflight_passed}/{preflight_total}`",
        f"Equity: `{_optional_money(account.get('equity_usdt'))}`",
        f"Available: `{_optional_money(account.get('available_usdt'))}`",
        f"Positions: `{open_positions}` · Registry `{int(runtime.get('registry_count', 0) or 0)}`",
        f"Candidates: `{int(queue.get('count', 0) or 0)}` · Pending orders `{int(orders.get('unresolved', 0) or 0)}`",
        f"Engines: `{int(runtime.get('engines_running', 0) or 0)}/{int(runtime.get('engines_total', 0) or 0)}`",
        f"Process: `PID {int(runtime.get('process_pid', 0) or 0)} · uptime {float(runtime.get('process_uptime_sec', 0.0) or 0.0) / 3600.0:.1f}h`",
        f"Closed PnL ({final_label}): `${float(pnl.get('net_pnl_usdt', 0.0) or 0.0):+.4f}`",
        f"Open PnL (OKX): `{_optional_money(positions.get('open_pnl_usdt'), signed=True)}`",
        "",
        f"Position source: `{source}` · age `{age_text}`",
        f"Telegram backlog: `{int(health.get('handler_backlog', 0) or 0)}` · loop p95 `{float((health.get('event_loop_lag_ms', {}) or {}).get('p95', 0.0) or 0.0):.0f}ms`",
        f"Updated: `{runtime.get('generated_at', '')}`",
    ])


def _production_balance_text(account: dict, runtime: dict) -> str:
    live = runtime.get("live_state", {}) or {}
    return "\n".join([
        f"*{_live_state_badge(live)} · BALANCE*",
        "",
        f"Equity: `{_optional_money(account.get('equity_usdt'), decimals=6)}`",
        f"Available: `{_optional_money(account.get('available_usdt'), decimals=6)}`",
        f"Cash: `{_optional_money(account.get('cash_usdt'), decimals=6)}`",
        f"Frozen: `{_optional_money(account.get('frozen_usdt'), decimals=6)}`",
        f"Isolated margin: `{_optional_money(account.get('isolated_margin_usdt'), decimals=6)}`",
        f"Unrealized PnL: `{_optional_money(account.get('unrealized_pnl_usdt'), signed=True, decimals=6)}`",
        f"Open risk at stop: `{_optional_money(account.get('open_risk_usdt'), decimals=6)}`",
        f"Reserved margin: `{_optional_money(account.get('reserved_margin_usdt'), decimals=6)}` ({int(account.get('pending_reservations', 0) or 0)})",
        "",
        "Source: `OKX authenticated account`",
        f"Updated: `{account.get('generated_at', '')}`",
    ])


def _production_pnl_text(pnl: dict, positions: dict) -> str:
    pending = int(pnl.get("reconciling", 0) or 0)
    status = "🟢 LIVE FINAL" if pending == 0 else "🟡 LIVE RECONCILING"
    return "\n".join([
        f"*{status} · PERFORMANCE*",
        "",
        f"Final closed trades: `{int(pnl.get('closed_final', 0) or 0)}`",
        f"Wins / Losses / Neutral: `{int(pnl.get('wins', 0) or 0)} / {int(pnl.get('losses', 0) or 0)} / {int(pnl.get('neutral', 0) or 0)}`",
        f"Gross PnL: `${float(pnl.get('gross_pnl_usdt', 0.0) or 0.0):+.6f}`",
        f"Fees: `${float(pnl.get('fees_usdt', 0.0) or 0.0):+.6f}`",
        f"Funding: `${float(pnl.get('funding_usdt', 0.0) or 0.0):+.6f}`",
        f"Net PnL: `${float(pnl.get('net_pnl_usdt', 0.0) or 0.0):+.6f}`",
        f"Open PnL (OKX): `${float(positions.get('open_pnl_usdt', 0.0) or 0.0):+.6f}`",
        f"Reconciling: `{pending}`",
        "",
        "Historical paper and counterfactual rows are excluded.",
        f"Source: `{pnl.get('source', 'pnl_reconciler_exchange_final')}`",
        f"Updated: `{pnl.get('generated_at_iso', '')}`",
    ])


def _production_trades_text(pnl: dict) -> str:
    rows = []
    for row in pnl.get("final_rows", []) or []:
        item = dict(row)
        item["display_status"] = "LIVE FINAL"
        rows.append(item)
    for row in pnl.get("pending_rows", []) or []:
        item = dict(row)
        item["display_status"] = "LIVE RECONCILING"
        rows.append(item)
    for row in pnl.get("mismatch_rows", []) or []:
        item = dict(row)
        item["display_status"] = "LIVE MISMATCH"
        rows.append(item)
    rows.sort(key=lambda row: float(row.get("exchange_confirmed_at", row.get("created_at", 0.0)) or 0.0), reverse=True)
    lines = ["*🔴 LIVE · RECENT TRADES*", ""]
    if not rows:
        lines.extend(["No production closes recorded.", "", "Historical paper and counterfactual rows are excluded."])
        return "\n".join(lines)
    for row in rows[:20]:
        coin = str(row.get("coin", row.get("inst_id", "?"))).split("-")[0].upper()
        direction = str(row.get("direction", "") or "").upper()
        status = str(row.get("display_status", "LIVE RECONCILING"))
        pnl_value = row.get("exchange_realized_pnl")
        if pnl_value is None:
            pnl_value = row.get("local_estimated_net_pnl")
        pnl_text = "—" if pnl_value is None else f"${float(pnl_value):+.6f}"
        lines.append(f"`{status}` · `{coin}` {direction} · `{pnl_text}`")
    return "\n".join(lines)


def _engine_text(name: str) -> str:
    name  = name.upper()
    eng   = engine.engines.get(name)
    if not eng:
        return f"Unknown engine: {name}"
    emoji = ENGINE_EMOJI.get(name, "")
    stats = get_engine_stats(name)
    perf = get_engine_recent_performance(name, 10, 30)
    mode = _settings.get_engine_mode(name)
    role = _settings.get_engine_role(name)
    mode_label = {"live": "● LIVE", "probation": "⚠ PROBATION", "shadow": "👻 SHADOW", "disabled": "✕ DISABLED"}.get(mode, mode.upper())
    lane = _settings.get_engine_lane(name)
    if eng.running:
        status_s = f"{mode_label} · Running"
    else:
        status_s = f"{mode_label} · Stopped"
    descs = STRATEGY_DESCRIPTIONS.get(name, [])
    lines = [
        f"{emoji} *{engine_display_name(name)}*  ·  {status_s}",
        f"  Lane: `{lane}`",
        f"  Role: `{role}`",
        "",
        f"  Positions   `{eng.trade_count()}`",
        f"  Leverage    `{_engine_target_leverage(name)}x`",
        "",
        "  Evidence    `HISTORICAL PAPER / COUNTERFACTUAL`",
        f"  Paper Today `{stats.get('today_wins', 0)}W  {stats.get('today_losses', 0)}L`"
        f"  `({stats.get('today_wr', 0.0)}%)`  `${stats.get('today_pnl', 0.0):+.4f}`",
        f"  Paper Week  `{stats.get('week_wins', 0)}W  {stats.get('week_losses', 0)}L`"
        f"  `${stats.get('week_pnl', 0.0):+.4f}`",
        f"  Paper Total `{stats.get('total_wins', 0)}W  {stats.get('total_losses', 0)}L`"
        f"  `${stats.get('total_pnl', 0.0):+.4f}`",
        f"  Rehab      L10 `${float(perf.get('last10_net', 0.0) or 0.0):+.2f}`  "
        f"L30 `${float(perf.get('last30_net', 0.0) or 0.0):+.2f}`  "
        f"Fee `{float(perf.get('fee_to_profit_ratio', 0.0) or 0.0):.2f}`  "
        f"Dup `{float(perf.get('duplicate_entry_rate', 0.0) or 0.0):.0f}%`",
    ]
    if descs:
        lines += ["", "*Signals:*"]
        for sname, sdesc in descs:
            lines.append(f"  · *{sname}* — _{sdesc}_")
    if name == "SHOGUN":
        lines.append(f"\n  Kill Zone: _{killzone_status()}_")
    if name == "TENJIN":
        lines.append(f"\n  SL `{_v('tenjin_sl_pct', 2.5):.1f}%`  TP `{_v('tenjin_tp_pct', 9.0):.1f}%`  ·  4H signals  ·  3/3 required")
    if name == "SUSANOO":
        lines.append(f"\n  Threshold `{_v('susanoo_funding_threshold', 0.0005) * 100:.3f}%`  ·  Fades crowded perps")
    if name == "AMATERASU":
        lines.append(f"\n  1m + 15m + 1H confluence  ·  Daily/Weekly macro weighting  ·  3/3 required")
    # Show shadow signal count for shadow-mode engines
    if mode == "shadow" and hasattr(eng, 'get_shadow_signals'):
        shadow_count = len(eng.get_shadow_signals(200))
        shadow_trades = int(_v(f"engine_{name.lower()}_shadow_trades", 0))
        lines.append(f"\n  👻 Shadow signals: `{shadow_count}` recorded, `{shadow_trades}` qualified")
    if name == "TITAN":
        lines.append(f"\n  Top-20 mcap  ·  Size `{_v('titan_size_mult_3votes', 2.0):.1f}x` on 3/3")
    if name == "IZANAGI":
        lines.append(f"\n  Composite scorer  ·  Score ≥ 0.35 = strong (3 pts)")
    if name == "HACHIMAN":
        lines.append(f"\n  Selector bonus `{_v('hachiman_selection_bonus', 6.0):.1f}`  ·  Top-bias `{_v('hachiman_top_bias_bonus', 3.0):.1f}`")
    if name == "YATAGARASU":
        lines.append(f"\n  Premium confluence  ·  breakout buffer `{_v('yatagarasu_breakout_buffer_pct', 0.35):.2f}%`  ·  volume `{_v('yatagarasu_volume_confirm_mult', 1.2):.2f}x`")
    if name == "RYUJIN":
        lines.append(f"\n  Flow continuation  ·  buy ratio `{_v('ryujin_flow_ratio_long', 0.60):.2f}` / `{_v('ryujin_flow_ratio_short', 0.40):.2f}`")
    if name == "KANNON":
        lines.append("\n  Recovery guardian  ·  advisory checks on recovered positions and exit-plan completeness")
    if name == "GENBU":
        lines.append(f"\n  Advisory governor  ·  soft quality floor `{_v('genbu_quality_floor', 55.0):.1f}`")
    positions = eng.open_positions()
    if positions:
        lines += ["", "*Open:*"]
        for coin, t in positions.items():
            side  = "↗" if t.get("side") == "buy" else "↘"
            entry = float(t.get("entry", 0))
            lev   = int(t.get("leverage", _v("leverage_default", 10)))
            pnl_s = "`see Positions`"
            lines.append(f"  {side} `{coin}`  {lev}x  `${entry:.6f}`  {pnl_s}")
    return "\n".join(lines)


def _positions_text(positions: dict, live_snapshot: dict | None = None) -> str:
    if live_snapshot is not None:
        text = _tp.panel_positions_truth_text(live_snapshot)
        if bool(live_snapshot.get("stale", False)) or str(live_snapshot.get("freshness_status", "")).upper() != "FRESH":
            source = live_snapshot.get("source_label") or live_snapshot.get("source") or "UNKNOWN"
            age = live_snapshot.get("age_sec")
            age_text = f"{float(age):.0f}s old" if age is not None else "age unknown"
            status = live_snapshot.get("refresh_status") or live_snapshot.get("refresh_error") or live_snapshot.get("freshness_status") or "STALE"
            return f"⚠️ *Position truth is {str(status).replace('_', ' ').lower()}* · `{source}` · `{age_text}`\n\n{text}"
        return text
    return "📊 *Positions*\n\n_Open PnL is syncing. Refresh shortly._" if positions else "📊 *Positions*\n\n_No open positions._"


def _terminal_opportunities_text(runtime: dict) -> str:
    import production_research_funnel as funnel
    latest = funnel.latest()
    counts = dict(latest.get("counts", {}) or {})
    queue = dict(runtime.get("opportunities", {}) or {})
    evidence = list(latest.get("recent_evidence", []) or [])[-5:]
    lines = [
        "*🔴 LIVE · OPPORTUNITIES*", "",
        f"Raw signals: `{int(counts.get('raw_signal', 0) or 0)}`",
        f"Pre-candidates: `{int(counts.get('pre_candidate', 0) or 0)}`",
        f"Candidates: `{int(counts.get('candidate', 0) or 0)}`",
        f"Queued: `{int(queue.get('count', 0) or 0)}`",
        f"Reservations: `{int(runtime.get('reservation_count', 0) or 0)}`",
    ]
    for row in reversed(evidence):
        lines.append(
            f"`{str(row.get('symbol', '?')).upper()}` {str(row.get('stage', '')).upper()} · "
            f"`{str(row.get('engine', '?')).upper()}` · `{str(row.get('reason', '') or 'accepted')}`"
        )
    lines.extend(["", f"Snapshot: `{latest.get('finished_at', latest.get('timestamp', 'not available'))}`"])
    return "\n".join(lines)


def _terminal_execution_text() -> str:
    from capital_reservations import book as reservations
    from execution_flow_telemetry import snapshot as flow_snapshot
    from execution_supervisor import supervisor
    from order_state_store import store as orders
    order_snap = orders.snapshot()
    flow = flow_snapshot(6)
    funnel = dict(flow.get("funnel_counters", {}) or {})
    active = supervisor.snapshot()
    lines = [
        "*🔴 LIVE · EXECUTION*", "",
        f"Execution workers: `{int(active.get('active_count', 0) or 0)}`",
        f"Reservations: `{len(reservations.snapshot())}`",
        f"Unresolved intents: `{int(order_snap.get('unresolved', 0) or 0)}`",
        "",
        "*Final pre-submit funnel*",
        f"Router requested: `{int(funnel.get('router_requested', 0) or 0)}`",
        f"Router validated: `{int(funnel.get('router_validated', 0) or 0)}`",
        f"Payload ready: `{int(funnel.get('payload_ready', 0) or 0)}`",
        f"Submitted to OKX: `{int(funnel.get('submission_attempted', 0) or 0)}`",
        f"Acknowledged by OKX: `{int(funnel.get('exchange_acknowledged', 0) or 0)}`",
        f"Filled: `{int(funnel.get('filled', 0) or 0)}`",
        "",
        "*Local pre-OKX blockers*",
        f"Zero contracts: `{int(funnel.get('rejected_zero_contracts', 0) or 0)}`",
        f"Min notional: `{int(funnel.get('rejected_min_notional', 0) or 0)}`",
        f"Candidate stale: `{int(funnel.get('rejected_candidate_stale', 0) or 0)}`",
        f"Market stale: `{int(funnel.get('rejected_market_stale', 0) or 0)}`",
        f"Account state: `{int(funnel.get('rejected_account_state', 0) or 0)}`",
        f"Reservation: `{int(funnel.get('rejected_reservation', 0) or 0)}`",
        f"Leverage: `{int(funnel.get('rejected_leverage', 0) or 0)}`",
        f"Position mode: `{int(funnel.get('rejected_position_mode', 0) or 0)}`",
        f"Economics/reprice: `{int(funnel.get('rejected_economics_after_reprice', 0) or 0)}`",
        f"Other: `{int(funnel.get('rejected_other', 0) or 0)}`",
        f"Exchange rejected: `{int(funnel.get('exchange_rejected', 0) or 0)}`",
        f"Exchange uncertain: `{int(funnel.get('exchange_uncertain', 0) or 0)}`",
        "", "Recent:",
    ]
    for row in flow.get("recent", [])[:6]:
        lines.append(
            f"`{row.get('coin', '?')}` `{row.get('stage', '?')}` · "
            f"`{row.get('terminal_class') or row.get('funnel_counter') or row.get('reason', '') or 'OK'}`"
        )
    return "\n".join(lines)


def _terminal_risk_text(runtime: dict) -> str:
    cfg = dict(runtime.get("settings", {}) or {})
    return "\n".join([
        "*🔴 LIVE · RISK & SIZING*", "",
        f"Max positions: `{cfg.get('max_open_positions', 0)}`",
        f"Aggregate open risk: `{float(_settings.get_val('rule_357_max_open_risk_pct', 0.0) or 0.0):.2f}%`",
        f"Daily loss cap: `{'ON' if cfg.get('daily_cap_enabled') else 'OFF'} · {float(cfg.get('daily_loss_pct', 0.0) or 0.0):.2f}%`",
        f"Weekly loss cap: `{'ON' if cfg.get('weekly_cap_enabled') else 'OFF'} · {float(cfg.get('weekly_loss_pct', 0.0) or 0.0):.2f}%`",
        f"Leverage bounds: `{cfg.get('leverage_min', 0)}-{cfg.get('leverage_max', 0)}x`",
        f"Risk sizing: `{'ON' if _settings.get_val('risk_sizing_enabled', True) else 'OFF'}`",
        f"Reserved capital: `{runtime.get('reservation_count', 0)}` reservations",
        "", "Production safety values are read from the effective settings store.",
    ])


def _terminal_system_text(runtime: dict, health: dict) -> str:
    scan = dict(runtime.get("scan", {}) or {})
    sync = engine.get_sync_health_snapshot()
    ws_ok = bool(engine.status_snapshot().get("ws_active"))
    finished = bool(scan.get("finished_at"))
    phase = "STARTUP WARM-UP" if int(scan.get("cycle_number", 1) or 1) == 1 and not finished else "SCANNING" if scan.get("started_at") and not finished else "READY"
    return "\n".join([
        "*🔴 LIVE · SYSTEM HEALTH*", "",
        f"Telegram: `{'HEALTHY' if int(health.get('handler_backlog', 0) or 0) == 0 else 'DEGRADED'}`",
        f"Market WS: `{'HEALTHY' if ws_ok else 'DEGRADED'}`",
        f"REST/Auth: `{'HEALTHY' if runtime.get('preflight', {}).get('ok') else 'FAILED'}`",
        f"Execution router: `HEALTHY`",
        f"Exit manager: `{'HEALTHY' if engine.running else 'DEGRADED'}`",
        f"Reconciliation: `{str(sync.get('status', 'UNKNOWN')).upper()}`",
        f"Engine runtime: `{runtime.get('engines_running', 0)}/{runtime.get('engines_total', 0)}`",
        f"Market phase: `{phase}`",
        f"Last completed scan: `{float(scan.get('duration_sec', 0.0) or 0.0):.2f}s`",
        f"Engine timeouts: `{len(scan.get('engine_timeouts', []) or [])}`",
        f"Telegram loop p95: `{float((health.get('event_loop_lag_ms', {}) or {}).get('p95', 0.0) or 0.0):.0f}ms`",
    ])


def _terminal_why_no_trade_text() -> str:
    import production_research_funnel as funnel
    from execution_flow_telemetry import snapshot as flow_snapshot
    row = funnel.latest()
    counts = dict(row.get("counts", {}) or {})
    reasons = sorted((row.get("reasons", {}) or {}).items(), key=lambda item: -int(item[1]))[:6]
    flow = flow_snapshot(0)
    funnel_c = dict(flow.get("funnel_counters", {}) or {})
    lines = [
        "*🔎 WHY NO TRADE?*", "", "Last completed scan:",
        f"Symbols: `{int(row.get('symbols_seen', 0) or 0)}`",
        f"Data ready: `{int(row.get('symbols_data_ready', 0) or 0)}`",
        f"Raw signals: `{int(counts.get('raw_signal', 0) or 0)}`",
        f"Pre-candidates: `{int(counts.get('pre_candidate', 0) or 0)}`",
        f"Candidates: `{int(counts.get('candidate', 0) or 0)}`",
        "",
        "*Final execution funnel*",
        f"Router requested: `{int(funnel_c.get('router_requested', 0) or 0)}`",
        f"Payload ready: `{int(funnel_c.get('payload_ready', 0) or 0)}`",
        f"Submitted to OKX: `{int(funnel_c.get('submission_attempted', 0) or 0)}`",
        f"Acknowledged by OKX: `{int(funnel_c.get('exchange_acknowledged', 0) or 0)}`",
        f"Filled: `{int(funnel_c.get('filled', 0) or 0)}`",
        "",
        "*Final local blockers*",
        f"Zero contracts: `{int(funnel_c.get('rejected_zero_contracts', 0) or 0)}`",
        f"Min notional: `{int(funnel_c.get('rejected_min_notional', 0) or 0)}`",
        f"Candidate stale: `{int(funnel_c.get('rejected_candidate_stale', 0) or 0)}`",
        f"Market stale: `{int(funnel_c.get('rejected_market_stale', 0) or 0)}`",
        f"Account / reservation / other: "
        f"`{int(funnel_c.get('rejected_account_state', 0) or 0)}` / "
        f"`{int(funnel_c.get('rejected_reservation', 0) or 0)}` / "
        f"`{int(funnel_c.get('rejected_other', 0) or 0)}`",
        "", "Top strategy blockers:",
    ]
    lines.extend(f"• `{reason}`: `{count}`" for reason, count in reasons)
    return "\n".join(lines)


# ── Commands ──────────────────────────────────────────────────────────────────

async def cmd_start(u, ctx):
    await cmd_status(u, ctx)

async def cmd_help(u, ctx):
    if not is_allowed(u.effective_user.id): return await _deny(u)
    text = (
        "🤖 *OxyPulse Control*\n\n"
        "The inline menu is the main operator UI.\n\n"
        "*Main views*\n"
        "  • *Home* shows balance, regime, session, open trades, and day/week performance.\n"
        "  • *Engines* shows each engine with status, PnL, controls, and open positions.\n"
        "  • *Reports* shows combined stats, win rate, trades, and engine breakdowns.\n"
        "  • *Settings* changes live leverage, risk, cooldown, and protections.\n\n"
        "*Core commands*\n"
        "`/start` `/status` `/startbot` `/stop`\n"
        "`/positions` `/balance` `/pnl` `/trades`\n"
        "`/engines` `/close BTC` `/closeall` `/sync`\n"
        "`/settings` `/winrate` `/blocked`\n\n"
        "*Notes*\n"
        "  • Menu callbacks are now the primary operator UI.\n"
        "  • `/sync` repairs registry drift against OKX.\n"
        "  • Plain text is ignored; use the menu or commands."
    )
    await u.message.reply_text(text, reply_markup=main_menu(), parse_mode="Markdown")


def _preflight_operator_presentation(preflight, snapshot, *, reason: str = "", read_only: bool = False) -> str:
    """Render only the sanitized diagnostic projection of a preflight failure."""
    from telegram_operator_controls import format_preflight_failure

    blocked = not bool(preflight.get("ok"))
    if read_only:
        lines = [
            "Live Preflight (read-only)",
            "",
            f"Result: `{'PASS' if not blocked else 'BLOCKED'}`",
        ]
        if blocked:
            lines.append(format_preflight_failure(preflight))
        lines.extend([
            f"State: `{snapshot.get('state', 'UNKNOWN')}`",
            f"Armed: `{'YES' if snapshot.get('armed') else 'NO'}`",
            "Orders submitted: `0`",
        ])
        return "\n".join(lines)
    lines = [
        _live_state_badge(snapshot),
        "",
        f"Preflight: `{'PASS' if not blocked else 'BLOCKED'}`",
    ]
    if blocked:
        lines.append(format_preflight_failure(preflight))
    lines.append(f"Reason: `{reason}`")
    return "\n".join(lines)


async def cmd_startbot(u, ctx):
    if not is_allowed(u.effective_user.id): return await _deny(u)
    from telegram_operator_controls import arm as _operator_arm
    result = await _operator_arm(okx, engine, invocation_source="telegram_startbot")
    await _tracked_reply(
        u.message,
        _preflight_operator_presentation(
            result["preflight"], result["state"], reason=str(result.get("reason", "")),
        ),
        reply_markup=main_menu(), parse_mode="Markdown",
    )

async def cmd_stopbot(u, ctx):
    if not is_allowed(u.effective_user.id): return await _deny(u)
    from telegram_operator_controls import disarm as _operator_disarm
    result = await _operator_disarm(engine)
    await _tracked_reply(
        u.message,
        f"{_live_state_badge(result['state'])}\n\nNew entries blocked. Exit management and reconciliation remain active.",
        reply_markup=main_menu(), parse_mode="Markdown",
    )

async def cmd_status(u, ctx):
    if not is_allowed(u.effective_user.id): return await _deny(u)
    runtime = await _tg_backend.control_runtime_snapshot(engine, _settings, priority="high")
    account, live_snapshot = await _tg_backend.fast_operational_exchange_snapshot(
        okx,
        _live_position_snapshot,
    )
    pnl = await _tg_backend.control_pnl_snapshot(priority="normal")
    health = _tg_backend.telegram_health_snapshot()
    await _tracked_reply(
        u.message,
        _production_status_text(runtime, account, live_snapshot, pnl, health),
        reply_markup=main_menu(),
        parse_mode="Markdown",
    )

async def cmd_balance(u, ctx):
    if not is_allowed(u.effective_user.id): return await _deny(u)
    try:
        account, live_snapshot = await _tg_backend.fast_operational_exchange_snapshot(
            okx,
            _live_position_snapshot,
        )
        runtime = await _tg_backend.control_runtime_snapshot(engine, _settings, priority="high")
        text = _production_balance_text(account, runtime)
    except Exception as exc:
        logger.exception("Telegram balance refresh failed")
        live = (await _tg_backend.control_runtime_snapshot(engine, _settings, priority="high")).get("live_state", {})
        text = f"⚠️ *Unable to refresh balance.*\n\nTrading state remains `{live.get('state', 'UNKNOWN')}`."
    await _tracked_reply(u.message, text, reply_markup=main_menu(), parse_mode="Markdown")

async def cmd_positions(u, ctx):
    if not is_allowed(u.effective_user.id): return await _deny(u)
    positions = {p["coin"]: p for p in engine.all_open_positions()}
    live_snapshot = await _tg_backend.fast_operational_exchange_snapshot(
        okx,
        lambda: _live_position_snapshot(force_refresh=True),
    )
    live_snapshot = live_snapshot[1]
    await _tracked_reply(
        u.message,
        _positions_text(positions, live_snapshot),
        reply_markup=positions_kb(positions, live_snapshot),
        parse_mode="Markdown",
    )

async def cmd_pnl(u, ctx):
    if not is_allowed(u.effective_user.id): return await _deny(u)
    pnl = await _tg_backend.control_pnl_snapshot(priority="normal")
    live_snapshot = await _live_position_snapshot()
    await _tracked_reply(
        u.message,
        _production_pnl_text(pnl, live_snapshot),
        reply_markup=reports_menu(),
        parse_mode="Markdown",
    )

async def cmd_sync(u, ctx):
    if not is_allowed(u.effective_user.id): return await _deny(u)
    await ctx.bot.send_chat_action(u.effective_chat.id, "typing")
    r = await engine.sync_positions()
    await u.message.reply_text(r, reply_markup=main_menu(), parse_mode="Markdown")

async def cmd_scan(u, ctx):
    if not is_allowed(u.effective_user.id): return await _deny(u)
    reg = reg_get_all()
    text = "📋 *Registry* — empty" if not reg else "📋 *Position Registry*\n\n" + "\n".join(f"  `{c}` — {info['direction'].upper()} [{ENGINE_EMOJI.get(info.get('engine',''),'🤖')}{info.get('engine','?')}]" for c, info in reg.items())
    await u.message.reply_text(text, reply_markup=main_menu(), parse_mode="Markdown")

async def cmd_close(u, ctx):
    if not is_allowed(u.effective_user.id): return await _deny(u)
    args = ctx.args or []
    if not args: await u.message.reply_text("Usage: `/close BTC`", parse_mode="Markdown"); return
    r = await engine.close_coin(args[0].upper())
    await u.message.reply_text(str(r), reply_markup=main_menu(), parse_mode="Markdown")

async def cmd_closeall(u, ctx):
    if not is_allowed(u.effective_user.id): return await _deny(u)
    await ctx.bot.send_chat_action(u.effective_chat.id, "typing")
    await u.message.reply_text(await engine.close_all_positions(), reply_markup=main_menu(), parse_mode="Markdown")

async def cmd_settings(u, ctx):
    if not is_allowed(u.effective_user.id): return await _deny(u)
    text, kb = await _settings_dashboard_payload()
    await u.message.reply_text(text, reply_markup=kb, parse_mode="Markdown")

async def cmd_v6(u, ctx):
    """Send a V6.1 system status report: capital mode, drawdown, exposure, flags."""
    if not is_allowed(u.effective_user.id): return await _deny(u)
    await ctx.bot.send_chat_action(u.effective_chat.id, "typing")
    try:
        lines = ["⚙️ *OxyPulse V6.1 Status*\n"]

        # ── Capital profile ────────────────────────────────────────────────
        try:
            bal = await okx.get_usdt_balance()
        except Exception:
            bal = 0.0
        v6_on = bool(_v("v6_enabled", False))
        lines.append(f"*V6 master gate:* `{'ON ✅' if v6_on else 'OFF ❌'}`")

        if _v("v6_capital_adaptive_enabled", True):
            from capital_manager import get_profile as _cm_get
            _prof = _cm_get(bal)
            mode_icons = {"MICRO": "🟢", "GROWTH": "🔵", "PRO": "🟣"}
            mi = mode_icons.get(_prof.mode, "⚪")
            lines.append(
                f"\n*Capital Mode:* {mi} `{_prof.mode}`\n"
                f"  Balance: `${bal:,.2f}`\n"
                f"  Size multiplier: `{_prof.size_multiplier:.2f}×`\n"
                f"  Risk multiplier: `{_prof.risk_multiplier:.2f}×`\n"
                f"  Confidence threshold: `{_prof.confidence_threshold:.1f}`\n"
                f"  Max trades/hr: `{_prof.max_trades_per_hour}`\n"
                f"  DD tolerance: `{_prof.drawdown_tolerance_pct:.1f}%`\n"
                f"  Aggressiveness: `{_prof.aggressiveness_score:.0%}`"
            )

        # ── Drawdown guard ─────────────────────────────────────────────────
        if _v("v6_anti_drawdown_enabled", False):
            from drawdown_guard import get_drawdown_state as _dd_get
            _prof_ref = None
            try:
                from capital_manager import get_profile as _cm_g2
                _prof_ref = _cm_g2(bal)
            except Exception:
                pass
            _dd = _dd_get(capital_profile=_prof_ref)
            dd_icons = {0: "🟢", 1: "🟡", 2: "🟠", 3: "🔴", 4: "🛑"}
            lines.append(
                f"\n*Drawdown Guard:* {dd_icons.get(_dd.level, '⚪')} Level `{_dd.level}` — `{_dd.label}`\n"
                f"  Daily DD: `{_dd.daily_dd_pct:.1f}%`  Weekly DD: `{_dd.weekly_dd_pct:.1f}%`\n"
                f"  Size mult: `{_dd.size_multiplier:.2f}×`  Allow new: `{_dd.allow_new_trades}`"
            )
        else:
            lines.append("\n*Drawdown Guard:* `disabled`")

        # ── Exposure ───────────────────────────────────────────────────────
        if _v("v6_correlation_control_enabled", True):
            from exposure_tracker import get_exposure as _exp_get
            _exp = _exp_get(bal)
            lines.append(
                f"\n*Exposure:* Long `${_exp.long_usdt:,.0f}` ({_exp.long_count}pos)  "
                f"Short `${_exp.short_usdt:,.0f}` ({_exp.short_count}pos)\n"
                f"  Net: `{_exp.net_exposure_pct:.1f}%` of balance"
            )

        # ── Feature flags ──────────────────────────────────────────────────
        flag_map = {
            "v6_weighted_confidence_enabled": "Weighted confidence",
            "v6_dynamic_sizing_enabled":      "Dynamic sizing",
            "v6_anti_drawdown_enabled":       "Anti-drawdown",
            "v6_smart_execution_enabled":     "Smart execution",
                "engine_weight_adaptation_enabled": "Engine adaptation",
            "v6_capital_adaptive_enabled":    "Capital adaptive",
            "v6_correlation_control_enabled": "Correlation control",
            "v6_trade_frequency_enabled":     "Frequency control",
            "v6_market_filter_enabled":       "Market filter",
        }
        flag_lines = []
        for key, label in flag_map.items():
            state = "✅" if _v(key, False) else "❌"
            flag_lines.append(f"  {state} {label}")
        lines.append("\n*Feature Flags:*\n" + "\n".join(flag_lines))

        text = "\n".join(lines)
    except Exception as e:
        text = f"⚠️ V6.1 status error: {e}"

    await u.message.reply_text(text, reply_markup=main_menu(), parse_mode="Markdown")


async def cmd_clearblackswan(u, ctx):
    if not is_allowed(u.effective_user.id): return await _deny(u)
    clear_blackswan()
    await u.message.reply_text("✅ Blackswan halt cleared.", reply_markup=main_menu())

async def cmd_reset(u, ctx):
    """Reset all performance tracking data (trades, stats, reports)."""
    if not is_allowed(u.effective_user.id): return await _deny(u)
    try:
        import json
        from pathlib import Path
        from dataclasses import asdict

        # ── 1. Clear on-disk category stats ───────────────────────────────────
        Path("data/category_stats.json").write_text("{}", encoding="utf-8")

        # ── 2. Write a clean performance.json with all PerformanceState fields ─
        # Must match the PerformanceState dataclass exactly — _load() will
        # deserialise it and overwrite the in-memory _state object.
        perf_data = {
            "trades":           [],
            "equity_peak":      0.0,
            "weekly_loss_usdt": 0.0,
            "weekly_reset_ts":  "",
            "consec_losses":    0,
            "size_halved":      False,
            "blackswan_halted": False,
            "total_fees_usdt":  0.0,
            "consec_wins":      0,
            "daily_gain_usdt":  0.0,
        }
        Path("data/performance.json").write_text(json.dumps(perf_data, indent=2, ensure_ascii=False), encoding="utf-8")

        # ── 3. Reload in-memory state immediately so bot uses fresh data ──────
        # Without this, the live _state object stays stale until next restart.
        _perf._load()
        logger.info("Reporting reset via Telegram command — in-memory state reloaded.")

        await u.message.reply_text(
            "✅ *Reporting Reset Complete*\n"
            "• Category stats cleared\n"
            "• Performance history cleared\n"
            "• `consec_losses`, `size_halved`, `equity_peak` all zeroed\n"
            "• In-memory state reloaded\n\n"
            "_New trading session starting fresh._",
            reply_markup=main_menu(),
            parse_mode="Markdown"
        )
    except Exception as e:
        await u.message.reply_text(f"⚠️ Reset failed: {e}", reply_markup=main_menu())
        logger.error("Reset command error: %s", e)

async def cmd_universe(u, ctx):
    if not is_allowed(u.effective_user.id): return await _deny(u)
    await ctx.bot.send_chat_action(u.effective_chat.id, "typing")
    try:
        uni = await get_futures_universe()
        text = f"🌐 *Universe* — {len(uni)} coins\n\n`" + "  ".join(uni[:30]) + "`"
    except Exception as e: text = f"⚠️ {e}"
    await u.message.reply_text(text, reply_markup=main_menu(), parse_mode="Markdown")

async def cmd_winrate(u, ctx):
    """Live win rate monitor — rolling windows, per-engine, trend arrows."""
    if not is_allowed(u.effective_user.id): return await _deny(u)
    await ctx.bot.send_chat_action(u.effective_chat.id, "typing")
    try:
        text = format_winrate_report()
    except Exception as e:
        text = f"⚠️ Win rate report error: {e}"
    await u.message.reply_text(text, reply_markup=main_menu(), parse_mode="Markdown")

async def cmd_blocked(u, ctx):
    """Show recent blocked candidates with rejection reasons."""
    if not is_allowed(u.effective_user.id): return await _deny(u)
    import rejection_log as _rjlog
    rejections = _rjlog.get_recent(25)
    text = _tp.panel_blocked_text(rejections)
    kb = InlineKeyboardMarkup([[
        InlineKeyboardButton("🔙 Home", callback_data="main_menu"),
    ]])
    await u.message.reply_text(text, reply_markup=kb, parse_mode="Markdown")


async def cmd_quality(u, ctx):
    if not is_allowed(u.effective_user.id): return await _deny(u)
    await u.message.reply_text(_tp.render_novo_quality_panel(_settings), reply_markup=main_menu(), parse_mode="Markdown")


async def cmd_liquidity(u, ctx):
    if not is_allowed(u.effective_user.id): return await _deny(u)
    await u.message.reply_text(_tp.render_novo_liquidity_panel(_settings), reply_markup=main_menu(), parse_mode="Markdown")


async def cmd_crt(u, ctx):
    if not is_allowed(u.effective_user.id): return await _deny(u)
    await u.message.reply_text(_tp.render_novo_crt_panel(_settings), reply_markup=main_menu(), parse_mode="Markdown")


async def cmd_bias(u, ctx):
    if not is_allowed(u.effective_user.id): return await _deny(u)
    await u.message.reply_text(_tp.render_novo_bias_panel(_settings), reply_markup=main_menu(), parse_mode="Markdown")


async def cmd_learning(u, ctx):
    if not is_allowed(u.effective_user.id): return await _deny(u)
    await u.message.reply_text(_tp.render_novo_learning_panel(_settings), reply_markup=main_menu(), parse_mode="Markdown")


async def cmd_gems(u, ctx):
    if not is_allowed(u.effective_user.id): return await _deny(u)
    await u.message.reply_text(_tp.render_novo_gems_panel(_settings), reply_markup=main_menu(), parse_mode="Markdown")


async def cmd_signaldiag(u, ctx):
    if not is_allowed(u.effective_user.id): return await _deny(u)
    await u.message.reply_text(_tp.render_silent_signal_diagnostics_panel(_settings), reply_markup=main_menu(), parse_mode="Markdown")

# ── Callbacks ─────────────────────────────────────────────────────────────────

async def callbacks(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query; uid = q.from_user.id; data = q.data
    if not is_allowed(uid): return await q.answer("⛔ Unauthorised", show_alert=True)
    try:
        await q.answer()
    except Exception:
        pass  # stale query (timeout during boot fetch) — safe to ignore

    async def edit(text, kb=None):
        token = _tg_request_token.get()
        text = _with_mode_banner(text)
        try:
            current_text = getattr(q.message, "text", None) or getattr(q.message, "caption", None)
            current_markup = getattr(q.message, "reply_markup", None)
            current_markup_dict = current_markup.to_dict() if hasattr(current_markup, "to_dict") else current_markup
            next_markup_dict = kb.to_dict() if hasattr(kb, "to_dict") else kb
            if current_text == text and current_markup_dict == next_markup_dict:
                if token and _tg_control_plane.is_pending(token):
                    _tg_control_plane.backend_complete(token)
                    _tg_control_plane.sent(token)
                return
        except Exception:
            pass
        try:
            if token:
                _tg_control_plane.backend_complete(token)
            if kb: await q.edit_message_text(text, reply_markup=kb, parse_mode="Markdown")
            else:  await q.edit_message_text(text, parse_mode="Markdown")
            if token and _tg_control_plane.is_pending(token):
                _tg_control_plane.sent(token)
        except Exception as edit_err:
            _tg_control_plane.count("telegram_callback_failures")
            logger.debug("Callback edit fallback triggered for %s: %s", data, edit_err)
            # Telegram Markdown is fragile: an unescaped "_" or "*" in dynamic content can cause
            # "Can't parse entities..." and make the button seem unresponsive. Prefer showing the
            # panel as plain text rather than failing silently.
            try:
                if "parse entities" in str(edit_err).lower():
                    if kb: await q.edit_message_text(text, reply_markup=kb)
                    else:  await q.edit_message_text(text)
                    if token and _tg_control_plane.is_pending(token):
                        _tg_control_plane.sent(token)
                    return
            except Exception:
                pass
            try:
                if kb:
                    await q.message.reply_text(text, reply_markup=kb, parse_mode="Markdown")
                else:
                    await q.message.reply_text(text, parse_mode="Markdown")
                if token and _tg_control_plane.is_pending(token):
                    _tg_control_plane.sent(token)
            except Exception as reply_err:
                logger.warning("Callback reply fallback failed for %s: %s", data, reply_err)
                try:
                    if "parse entities" in str(reply_err).lower():
                        if kb: await q.message.reply_text(text, reply_markup=kb)
                        else:  await q.message.reply_text(text)
                        if token and _tg_control_plane.is_pending(token):
                            _tg_control_plane.sent(token)
                except Exception:
                    if token and _tg_control_plane.is_pending(token):
                        _tg_control_plane.error(token, reply_err, api_failure=True)

    # ── Room rendering helpers (one-setting-per-room) ───────────────────────
    _BOOL_ROOMS = {
        "tsl_enabled": dict(
            title="📈 Trailing Enabled",
            desc="Enable/disable trailing updates for exit management.",
            back_feature="set:fg:trailing",
            back_category="set:cat:execution",
        ),
        "breakeven_enabled": dict(
            title="🧷 Break-even Enabled",
            desc="Enable/disable break-even SL moves.",
            back_feature="set:fg:breakeven",
            back_category="set:cat:execution",
        ),
        "partial_exit_enabled": dict(
            title="💼 Partials Enabled",
            desc="Enable/disable milestone partial exits.",
            back_feature="set:fg:partials",
            back_category="set:cat:execution",
        ),
        "profit_lock_enabled": dict(
            title="🔒 Profit Lock Enabled",
            desc="Enable/disable profit-lock SL stages.",
            back_feature="set:fg:profit_lock",
            back_category="set:cat:execution",
        ),
        "early_invalidation_enabled": dict(
            title="Early Invalidation Enabled",
            desc="Master switch for lifecycle early-invalidation exits.",
            back_feature="set:fg:early_invalidation",
            back_category="set:cat:execution",
        ),
        "adaptive_early_invalidation_enabled": dict(
            title="Adaptive Early Invalidation",
            desc="Use scored failure-versus-patience evidence before invalidating.",
            back_feature="set:fg:early_invalidation",
            back_category="set:cat:execution",
        ),
        "profitability_telemetry_enabled": dict(
            title="Invalidation Telemetry",
            desc="Record lifecycle diagnostics, including invalidation outcomes.",
            back_feature="set:fg:early_invalidation",
            back_category="set:cat:execution",
        ),
        "early_invalidation_near_tp_grace_enabled": dict(
            title="Near-TP Grace",
            desc="Allow earned near-floor TP states to receive extra protection time.",
            back_feature="set:fg:early_invalidation",
            back_category="set:cat:execution",
        ),
        "symbol_direction_loss_cooldown_enabled": dict(
            title="Repeat Loss Cooling",
            desc="Apply localized cooling to repeated same-symbol same-direction losses.",
            back_feature="set:fg:repeat_loss_cooling",
            back_category="set:cat:execution",
        ),
        "realized_risk_ceiling_enabled": dict(
            title="🧯 Realized Risk Ceiling",
            desc="Cap expected loss at SL by resizing (and optionally tightening SL).",
            back_feature="set:fg:risk_ceiling",
            back_category="set:cat:risk",
        ),
        "realized_risk_ceiling_tighten_sl_if_needed": dict(
            title="🧯 Tighten SL If Needed",
            desc="If size cannot be reduced below executable minimum, tighten SL to satisfy risk cap.",
            back_feature="set:fg:risk_ceiling",
            back_category="set:cat:risk",
        ),
        "realized_risk_ceiling_telemetry_enabled": dict(
            title="🧯 Risk Ceiling Telemetry",
            desc="Log expected vs capped risk and any size/SL adjustments.",
            back_feature="set:fg:risk_ceiling",
            back_category="set:cat:risk",
        ),
        "short_rehab_enabled": dict(
            title="🩹 Short Rehab",
            desc="Apply mild size-only rehab for shorts in weak/choppy conditions.",
            back_feature="set:fg:short_rehab",
            back_category="set:cat:risk",
        ),
        "amaterasu_macro_layer_enabled": dict(
            title="☀️ AMATERASU Macro Layer",
            desc="Daily/weekly context weighting (non-blocking).",
            back_feature="set:fg:amaterasu_macro",
            back_category="set:cat:strategy",
        ),
        "fee_aware_partial_deferral_enabled": dict(
            title="Fee-Aware TP Deferral",
            desc="Defer fee-inefficient partial closes instead of taking dust profits.",
            back_feature="set:fg:partials",
            back_category="set:cat:execution",
        ),
        "continuation_runner_enabled": dict(
            title="Continuation Runner",
            desc="Preserve runner core when continuation quality is strong.",
            back_feature="set:fg:trailing",
            back_category="set:cat:execution",
        ),
        "exhaustion_assisted_tightening_enabled": dict(
            title="Exhaustion Tightening",
            desc="Tighten runners sooner when continuation quality fades.",
            back_feature="set:fg:trailing",
            back_category="set:cat:execution",
        ),
        "dynamic_sizing_enabled": dict(
            title="📏 Dynamic Sizing Enabled",
            desc="Enable/disable the live sizing overlay used for new trades.",
            back_feature="set:fg:deployment",
            back_category="set:cat:capital",
        ),
        "v6_telemetry_enabled": dict(
            title="📡 Telemetry Enabled",
            desc="Structured decision telemetry and diagnostics.",
            back_feature="set:cat:diagnostics",
            back_category="set:cat:diagnostics",
        ),
        "block_diagnostics_enabled": dict(
            title="🧾 Block Detail Enabled",
            desc="Show detailed block explanations and suggestions.",
            back_feature="set:cat:diagnostics",
            back_category="set:cat:diagnostics",
        ),
        "engine_weight_adaptation_enabled": dict(
            title="⚖️ Engine Weight Adaptation",
            desc="Adaptive engine weights based on performance.",
            back_feature="set:cat:strategy",
            back_category="set:cat:strategy",
        ),
    }

    _NUM_ROOMS = {
        "trailing_activation_pct": dict(
            title="📈 Trailing Activation %",
            desc="Start runner trailing after this leveraged ROI% is confirmed.",
            step=0.1,
            min=0.0,
            max=25.0,
            fmt="{v:.2f}",
            back_feature="set:fg:trailing",
            back_category="set:cat:execution",
        ),
        "trailing_distance_pct": dict(
            title="📈 Trailing Distance %",
            desc="Base runner trail distance used before TP tightening.",
            step=0.1,
            min=0.0,
            max=25.0,
            fmt="{v:.2f}",
            back_feature="set:fg:trailing",
            back_category="set:cat:execution",
        ),
        "trailing_after_tp1_pct": dict(
            title="📈 Trail After TP1 %",
            desc="Tighter runner trail distance after TP1 has executed.",
            step=0.05,
            min=0.0,
            max=25.0,
            fmt="{v:.2f}",
            back_feature="set:fg:trailing",
            back_category="set:cat:execution",
        ),
        "trailing_after_tp2_pct": dict(
            title="📈 Trail After TP2 %",
            desc="Final runner trail distance after TP2 has executed.",
            step=0.05,
            min=0.0,
            max=25.0,
            fmt="{v:.2f}",
            back_feature="set:fg:trailing",
            back_category="set:cat:execution",
        ),
        "runner_core_min_remaining_pct": dict(
            title="Runner Core %",
            desc="Minimum remaining runner core to preserve when exits are healthy.",
            step=5.0,
            min=0.0,
            max=100.0,
            fmt="{v:.1f}",
            back_feature="set:fg:trailing",
            back_category="set:cat:execution",
        ),
        "continuation_runner_core_min_pct": dict(
            title="Continuation Core %",
            desc="Runner core floor used when continuation quality is strong.",
            step=5.0,
            min=0.0,
            max=100.0,
            fmt="{v:.1f}",
            back_feature="set:fg:trailing",
            back_category="set:cat:execution",
        ),
        "continuation_runner_min_roi": dict(
            title="Continuation Min ROI %",
            desc="Minimum ROI before continuation runner preservation can engage.",
            step=0.1,
            min=0.0,
            max=100.0,
            fmt="{v:.2f}",
            back_feature="set:fg:trailing",
            back_category="set:cat:execution",
        ),
        "continuation_runner_score_threshold": dict(
            title="Continuation Score",
            desc="Continuation quality threshold for runner preservation.",
            step=0.25,
            min=0.0,
            max=10.0,
            fmt="{v:.2f}",
            back_feature="set:fg:trailing",
            back_category="set:cat:execution",
        ),
        "realized_risk_cap_pct_balance": dict(
            title="🧯 Risk Cap (% Balance)",
            desc="Expected loss cap at SL as % of balance (bounded by min/max USDT).",
            step=0.1,
            min=0.0,
            max=5.0,
            fmt="{v:.2f}",
            back_feature="set:fg:risk_ceiling",
            back_category="set:cat:risk",
        ),
        "realized_risk_cap_min_usdt": dict(
            title="🧯 Risk Cap Min (USDT)",
            desc="Absolute minimum USDT cap for expected loss at SL.",
            step=0.05,
            min=0.0,
            max=25.0,
            fmt="{v:.2f}",
            back_feature="set:fg:risk_ceiling",
            back_category="set:cat:risk",
        ),
        "realized_risk_cap_max_usdt": dict(
            title="🧯 Risk Cap Max (USDT)",
            desc="Absolute maximum USDT cap for expected loss at SL.",
            step=0.05,
            min=0.0,
            max=50.0,
            fmt="{v:.2f}",
            back_feature="set:fg:risk_ceiling",
            back_category="set:cat:risk",
        ),
        "short_rehab_weak_regime_size_mult": dict(
            title="🩹 Short Rehab Size×",
            desc="Size multiplier applied to shorts when conditions are weak/choppy.",
            step=0.01,
            min=0.25,
            max=1.0,
            fmt="{v:.2f}",
            back_feature="set:fg:short_rehab",
            back_category="set:cat:risk",
        ),
        "short_rehab_min_adx_1h": dict(
            title="🩹 Short Rehab ADX Floor",
            desc="If 1H ADX is below this, shorts receive mild size rehab (non-blocking).",
            step=0.5,
            min=0.0,
            max=60.0,
            fmt="{v:.1f}",
            back_feature="set:fg:short_rehab",
            back_category="set:cat:risk",
        ),
        "amaterasu_macro_conf_boost": dict(
            title="☀️ Macro Conf Boost",
            desc="Additive confidence hint when daily+weekly align (non-veto).",
            step=0.01,
            min=0.0,
            max=1.0,
            fmt="{v:.2f}",
            back_feature="set:fg:amaterasu_macro",
            back_category="set:cat:strategy",
        ),
        "amaterasu_macro_conf_penalty": dict(
            title="☀️ Macro Conf Penalty",
            desc="Confidence reduction when macro conflicts (non-veto).",
            step=0.01,
            min=0.0,
            max=1.0,
            fmt="{v:.2f}",
            back_feature="set:fg:amaterasu_macro",
            back_category="set:cat:strategy",
        ),
        "amaterasu_macro_size_boost": dict(
            title="☀️ Macro Size Boost",
            desc="Size multiplier when daily+weekly align.",
            step=0.01,
            min=0.5,
            max=2.0,
            fmt="{v:.2f}",
            back_feature="set:fg:amaterasu_macro",
            back_category="set:cat:strategy",
        ),
        "amaterasu_macro_size_penalty": dict(
            title="☀️ Macro Size Penalty",
            desc="Size multiplier when macro conflicts.",
            step=0.01,
            min=0.5,
            max=1.0,
            fmt="{v:.2f}",
            back_feature="set:fg:amaterasu_macro",
            back_category="set:cat:strategy",
        ),
        "continuation_runner_tp2_close_mult": dict(
            title="TP2 Close Multiplier",
            desc="Multiplier applied to TP2 trim size under strong continuation.",
            step=0.05,
            min=0.0,
            max=1.0,
            fmt="{v:.2f}",
            back_feature="set:fg:trailing",
            back_category="set:cat:execution",
        ),
        "continuation_runner_trail_mult": dict(
            title="Trail Width Multiplier",
            desc="Multiplier applied to runner trail distance under continuation.",
            step=0.05,
            min=0.1,
            max=10.0,
            fmt="{v:.2f}",
            back_feature="set:fg:trailing",
            back_category="set:cat:execution",
        ),
        "exhaustion_tighten_score_threshold": dict(
            title="Exhaustion Score",
            desc="Score threshold that allows runner tightening when enabled.",
            step=0.25,
            min=0.0,
            max=10.0,
            fmt="{v:.2f}",
            back_feature="set:fg:trailing",
            back_category="set:cat:execution",
        ),
        "exhaustion_tighten_trail_mult": dict(
            title="Exhaustion Trail Multiplier",
            desc="Trail multiplier used when exhaustion-assisted tightening engages.",
            step=0.05,
            min=0.1,
            max=1.5,
            fmt="{v:.2f}",
            back_feature="set:fg:trailing",
            back_category="set:cat:execution",
        ),
        "position_size_usdt": dict(
            title="💵 Base Trade Size ($)",
            desc="Base notional used before dynamic sizing and deployment caps.",
            step=5.0,
            min=1.0,
            max=5000.0,
            fmt="{v:.2f}",
            back_feature="set:fg:deployment",
            back_category="set:cat:capital",
        ),
        "position_size_pct": dict(
            title="💵 Balance-Based Size %",
            desc="Percent-of-balance base size contribution for new trades.",
            step=0.05,
            min=0.0,
            max=5.0,
            fmt="{v:.2f}",
            back_feature="set:fg:deployment",
            back_category="set:cat:capital",
        ),
        "v6_min_position_size_usdt": dict(
            title="💵 Min Size ($)",
            desc="Minimum executable notional floor after sizing penalties.",
            step=1.0,
            min=1.0,
            max=5000.0,
            fmt="{v:.2f}",
            back_feature="set:fg:deployment",
            back_category="set:cat:capital",
        ),
        "v6_max_position_size_usdt": dict(
            title="💵 Max Size ($)",
            desc="Maximum notional cap before margin and participation caps.",
            step=5.0,
            min=1.0,
            max=10000.0,
            fmt="{v:.2f}",
            back_feature="set:fg:deployment",
            back_category="set:cat:capital",
        ),
        "size_mode": dict(
            title="💵 Size Mode",
            desc="Controls how the base trade size is derived for new entries.",
            step=1.0,
            min=0.0,
            max=3.0,
            fmt="{v}",
            back_feature="set:fg:size",
            back_category="set:cat:capital",
        ),
        "v6_max_position_pct_of_balance": dict(
            title="💵 Balance Cap %",
            desc="Maximum per-trade margin budget as a fraction of balance.",
            step=0.01,
            min=0.0,
            max=1.0,
            fmt="{v:.2f}",
            back_feature="set:fg:deployment",
            back_category="set:cat:capital",
        ),
        "risk_pct": dict(
            title="💵 Risk % Size",
            desc="Risk budget used by the risk-based sizing mode.",
            step=0.05,
            min=0.0,
            max=25.0,
            fmt="{v:.2f}",
            back_feature="set:fg:size",
            back_category="set:cat:capital",
        ),
        "breakeven_trigger_pct": dict(
            title="🧷 Break-even Trigger %",
            desc="Leveraged ROI% required before break-even can activate.",
            step=0.1,
            min=0.0,
            max=50.0,
            fmt="{v:.2f}",
            back_feature="set:fg:breakeven",
            back_category="set:cat:execution",
        ),
        "breakeven_activation_profit_usd": dict(
            title="🧷 Break-even Activation ($)",
            desc="Activate break-even when unrealized profit reaches this $ amount.",
            step=0.05,
            min=0.0,
            max=100.0,
            fmt="{v:.2f}",
            back_feature="set:fg:breakeven",
            back_category="set:cat:execution",
        ),
        "breakeven_buffer_pct": dict(
            title="🧷 Break-even Buffer %",
            desc="Buffer added to break-even SL (percentage).",
            step=0.01,
            min=0.0,
            max=1.0,
            fmt="{v:.2f}",
            back_feature="set:fg:breakeven",
            back_category="set:cat:execution",
        ),
        "partial_tp1_close_pct": dict(
            title="💼 TP1 Close %",
            desc="Percent of position to close at TP1 milestone.",
            step=5.0,
            min=0.0,
            max=90.0,
            fmt="{v:.1f}",
            back_feature="set:fg:partials",
            back_category="set:cat:execution",
        ),
        "partial_tp2_close_pct": dict(
            title="💼 TP2 Close %",
            desc="Percent of position to close at TP2 milestone.",
            step=5.0,
            min=0.0,
            max=90.0,
            fmt="{v:.1f}",
            back_feature="set:fg:partials",
            back_category="set:cat:execution",
        ),
        "tp1_trigger_pct": dict(
            title="💼 TP1 Trigger %",
            desc="Leveraged ROI% required before TP1 partial can execute.",
            step=0.1,
            min=0.0,
            max=100.0,
            fmt="{v:.2f}",
            back_feature="set:fg:partials",
            back_category="set:cat:execution",
        ),
        "tp2_trigger_pct": dict(
            title="💼 TP2 Trigger %",
            desc="Leveraged ROI% required before TP2 partial can execute.",
            step=0.1,
            min=0.0,
            max=200.0,
            fmt="{v:.2f}",
            back_feature="set:fg:partials",
            back_category="set:cat:execution",
        ),
        "min_net_take_profit_usdt": dict(
            title="💼 Min Partial Profit ($)",
            desc="Minimum allowed realized profit for a partial close after fees.",
            step=0.01,
            min=0.0,
            max=25.0,
            fmt="{v:.2f}",
            back_feature="set:fg:partials",
            back_category="set:cat:execution",
        ),
        "min_full_close_profit_usdt": dict(
            title="💼 Min Full-Close Profit ($)",
            desc="Minimum meaningful protected profit before full close protection tightens.",
            step=0.01,
            min=0.0,
            max=25.0,
            fmt="{v:.2f}",
            back_feature="set:fg:partials",
            back_category="set:cat:execution",
        ),
        "minimum_net_capture_fee_multiple": dict(
            title="💰 Minimum Net Capture",
            desc="Minimum economically retained net profit for discretionary capture/protection, relative to estimated transaction costs.",
            step=0.25,
            min=0.0,
            fmt="{v:.2f}× fees",
            custom=True,
            back_feature="set:fg:profit_lock",
            back_category="set:cat:execution",
        ),
        "partial_skip_log_cooldown_sec": dict(
            title="Deferred TP Log Cooldown",
            desc="Seconds between repeated deferred partial logs for the same trade.",
            step=15.0,
            min=0.0,
            max=43200.0,
            fmt="{v:.0f}",
            back_feature="set:fg:partials",
            back_category="set:cat:execution",
        ),
        "economic_min_runner_notional_usdt": dict(
            title="Min Runner Notional ($)",
            desc="Minimum meaningful runner notional retained after partials.",
            step=1.0,
            min=0.0,
            max=1000000.0,
            fmt="{v:.2f}",
            back_feature="set:fg:partials",
            back_category="set:cat:execution",
        ),
        "economic_min_fee_mult": dict(
            title="Fee Burden Multiplier",
            desc="Economic minimum multiplier over estimated fee burden.",
            step=0.5,
            min=0.0,
            max=100.0,
            fmt="{v:.2f}",
            back_feature="set:fg:partials",
            back_category="set:cat:execution",
        ),
        "early_invalidation_base_min_hold_sec": dict(
            title="Base Hold Seconds",
            desc="Base patience window before adaptive invalidation can trigger.",
            step=5.0,
            min=0.0,
            max=3600.0,
            fmt="{v:.0f}",
            back_feature="set:fg:early_invalidation",
            back_category="set:cat:execution",
        ),
        "early_invalidation_min_hold_floor_sec": dict(
            title="Min Hold Floor Seconds",
            desc="Hard lower bound for adaptive invalidation patience.",
            step=5.0,
            min=0.0,
            max=3600.0,
            fmt="{v:.0f}",
            back_feature="set:fg:early_invalidation",
            back_category="set:cat:execution",
        ),
        "early_invalidation_max_hold_cap_sec": dict(
            title="Max Hold Cap Seconds",
            desc="Hard upper bound for adaptive invalidation patience.",
            step=30.0,
            min=0.0,
            max=7200.0,
            fmt="{v:.0f}",
            back_feature="set:fg:early_invalidation",
            back_category="set:cat:execution",
        ),
        "early_invalidation_failure_score_threshold": dict(
            title="Failure Score Threshold",
            desc="Minimum failure score needed before invalidation is allowed.",
            step=0.25,
            min=0.0,
            max=10.0,
            fmt="{v:.2f}",
            back_feature="set:fg:early_invalidation",
            back_category="set:cat:execution",
        ),
        "early_invalidation_patience_score_threshold": dict(
            title="Patience Score Threshold",
            desc="Patience score that must be clearly outweighed by failure evidence.",
            step=0.25,
            min=0.0,
            max=10.0,
            fmt="{v:.2f}",
            back_feature="set:fg:early_invalidation",
            back_category="set:cat:execution",
        ),
        "early_invalidation_near_tp_floor_ratio": dict(
            title="Near-TP Floor Ratio",
            desc="TP-floor ratio that marks near-floor TP as earned.",
            step=0.01,
            min=0.0,
            max=1.0,
            fmt="{v:.2f}",
            back_feature="set:fg:early_invalidation",
            back_category="set:cat:execution",
        ),
        "early_invalidation_near_tp_grace_sec": dict(
            title="Near-TP Grace Seconds",
            desc="Extra grace window after near-floor TP has been earned.",
            step=15.0,
            min=0.0,
            max=3600.0,
            fmt="{v:.0f}",
            back_feature="set:fg:early_invalidation",
            back_category="set:cat:execution",
        ),
        "early_invalidation_chop_strictness_mult": dict(
            title="Chop Strictness Multiplier",
            desc="Patience compression multiplier used in choppy regimes.",
            step=0.05,
            min=0.1,
            max=5.0,
            fmt="{v:.2f}",
            back_feature="set:fg:early_invalidation",
            back_category="set:cat:execution",
        ),
        "early_invalidation_trend_patience_mult": dict(
            title="Trend Patience Multiplier",
            desc="Patience expansion multiplier used in strong trends.",
            step=0.05,
            min=0.1,
            max=5.0,
            fmt="{v:.2f}",
            back_feature="set:fg:early_invalidation",
            back_category="set:cat:execution",
        ),
        "symbol_direction_loss_threshold": dict(
            title="Repeat Loss Threshold",
            desc="Same-symbol same-direction losses before localized cooling applies.",
            step=1.0,
            min=1.0,
            max=20.0,
            fmt="{v:.0f}",
            back_feature="set:fg:repeat_loss_cooling",
            back_category="set:cat:execution",
            as_int=True,
        ),
        "symbol_direction_cooldown_minutes": dict(
            title="Cooldown Minutes",
            desc="Duration of localized same-symbol same-direction cooling.",
            step=5.0,
            min=0.0,
            max=1440.0,
            fmt="{v:.0f}",
            back_feature="set:fg:repeat_loss_cooling",
            back_category="set:cat:execution",
        ),
        "symbol_direction_loss_conf_bump": dict(
            title="Confirmation Bump",
            desc="Extra confidence required only for the cooled symbol-direction pair.",
            step=0.05,
            min=0.0,
            max=5.0,
            fmt="{v:.2f}",
            back_feature="set:fg:repeat_loss_cooling",
            back_category="set:cat:execution",
        ),
        "symbol_direction_loss_size_mult": dict(
            title="Size Multiplier",
            desc="Localized size multiplier during repeat-loss cooling.",
            step=0.05,
            min=0.05,
            max=1.0,
            fmt="{v:.2f}",
            back_feature="set:fg:repeat_loss_cooling",
            back_category="set:cat:execution",
        ),
        "symbol_direction_memory_decay_hours": dict(
            title="Memory Decay Hours",
            desc="Hours before localized loss memory decays.",
            step=1.0,
            min=0.0,
            max=720.0,
            fmt="{v:.0f}",
            back_feature="set:fg:repeat_loss_cooling",
            back_category="set:cat:execution",
        ),
        "profit_lock_trigger_pct": dict(
            title="🔒 Profit Lock Trigger %",
            desc="Leveraged ROI% required before profit lock activates.",
            step=0.1,
            min=0.0,
            max=100.0,
            fmt="{v:.2f}",
            back_feature="set:fg:profit_lock",
            back_category="set:cat:execution",
        ),
        "profit_lock_buffer_pct": dict(
            title="🔒 Profit Lock Buffer %",
            desc="Leveraged ROI% to lock once profit lock activates.",
            step=0.05,
            min=0.0,
            max=50.0,
            fmt="{v:.2f}",
            back_feature="set:fg:profit_lock",
            back_category="set:cat:execution",
        ),
        "deployment_target_utilization_pct": dict(
            title="🎯 Utilization Target %",
            desc="Target deployable utilization (0 = profile default).",
            step=5.0,
            min=0.0,
            max=95.0,
            fmt="{v:.1f}",
            back_feature="set:fg:deployment",
            back_category="set:cat:capital",
        ),
        "deployment_max_positions": dict(
            title="📦 Deploy Max Positions",
            desc="Override deployment max positions (0 = profile default).",
            step=1.0,
            min=0.0,
            max=9999.0,
            fmt="{v:.0f}",
            back_feature="set:fg:deployment",
            back_category="set:cat:capital",
            as_int=True,
        ),
        "min_vote_score": dict(
            title="🧮 Min Vote Score",
            desc="Minimum vote score required for entry qualification.",
            step=0.1,
            min=1.0,
            max=3.0,
            fmt="{v:.1f}",
            back_feature="set:fg:confidence",
            back_category="set:cat:strategy",
        ),
        "v6_micro_confidence_threshold": dict(title="🧮 V6 Micro Threshold", desc="Confidence threshold for micro accounts.", step=0.05, min=0.0, max=10.0, fmt="{v:.2f}", back_feature="set:fg:confidence", back_category="set:cat:strategy"),
        "v6_growth_confidence_threshold": dict(title="🧮 V6 Growth Threshold", desc="Confidence threshold for growth accounts.", step=0.05, min=0.0, max=10.0, fmt="{v:.2f}", back_feature="set:fg:confidence", back_category="set:cat:strategy"),
        "v6_pro_confidence_threshold": dict(title="🧮 V6 Pro Threshold", desc="Confidence threshold for pro accounts.", step=0.05, min=0.0, max=10.0, fmt="{v:.2f}", back_feature="set:fg:confidence", back_category="set:cat:strategy"),
    }

    async def _render_bool_room(key: str) -> None:
        meta = _BOOL_ROOMS.get(key)
        if not meta:
            return
        on = bool(_v(key, False))
        await edit(
            _tp.panel_room_bool(_settings, key, meta["title"], meta["desc"]),
            _tp.kb_room_bool(
                on=on,
                cb_on=f"room:bool:{key}:on",
                cb_off=f"room:bool:{key}:off",
                back_feature=meta["back_feature"],
                back_category=meta["back_category"],
            ),
        )

    async def _render_num_room(key: str) -> None:
        meta = _NUM_ROOMS.get(key)
        if not meta:
            return
        await edit(
            _tp.panel_room_number(_settings, key, meta["title"], meta["desc"], fmt=meta.get("fmt", "{v}")),
            _tp.kb_room_number(
                cb_dn=f"room:num:{key}:dn",
                cb_up=f"room:num:{key}:up",
                cb_custom=(f"room:num:{key}:custom" if meta.get("custom") else None),
                back_feature=meta["back_feature"],
                back_category=meta["back_category"],
            ),
        )

    if data == "main_menu":
        runtime = await _tg_backend.control_runtime_snapshot(engine, _settings, priority="high")
        account, live_snapshot = await _tg_backend.fast_operational_exchange_snapshot(
            okx,
            _live_position_snapshot,
        )
        await edit(
            _production_status_text(
                runtime,
                account,
                live_snapshot,
                await _tg_backend.control_pnl_snapshot(priority="normal"),
                _tg_backend.telegram_health_snapshot(),
            ),
            _tp.kb_home(engine.running),
        )
    elif data == "terminal:status":
        runtime = await _tg_backend.control_runtime_snapshot(engine, _settings, priority="high")
        account, positions = await _tg_backend.fast_operational_exchange_snapshot(okx, _live_position_snapshot)
        await edit(
            _production_status_text(runtime, account, positions, await _tg_backend.control_pnl_snapshot(priority="normal"), _tg_backend.telegram_health_snapshot()),
            _tp.kb_terminal_back(),
        )
    elif data == "terminal:account":
        account, _positions = await _tg_backend.fast_operational_exchange_snapshot(okx, _live_position_snapshot)
        await edit(_production_balance_text(account, await _tg_backend.control_runtime_snapshot(engine, _settings, priority="high")), _tp.kb_terminal_back())
    elif data == "terminal:positions":
        local_positions = {p["coin"]: p for p in engine.all_open_positions()}
        _account, positions = await _tg_backend.fast_operational_exchange_snapshot(
            okx,
            lambda: _live_position_snapshot(force_refresh=True),
        )
        await edit(_positions_text(local_positions, positions), positions_kb(local_positions, positions))
    elif data == "terminal:opportunities":
        await edit(_terminal_opportunities_text(await _tg_backend.control_runtime_snapshot(engine, _settings, priority="normal")), _tp.kb_terminal_back())
    elif data == "terminal:engines":
        funnel = __import__("production_research_funnel").latest()
        by_engine = dict(funnel.get("by_engine", {}) or {})
        lines = ["*🔴 LIVE · ENGINES*", "", f"Active: `{engine.running_count()}/{len(engine.engines)}`"]
        for name, eng in engine.engines.items():
            counts = dict((by_engine.get(name, {}) or {}).get("counts", {}) or {})
            lines.append(
                f"`{name}` · `{'RUN' if eng.running else 'STOP'}` · "
                f"signals `{counts.get('raw_signal', 0)}` · candidates `{counts.get('candidate', 0)}`"
            )
        await edit("\n".join(lines), engines_menu())
    elif data == "terminal:execution":
        await edit(_terminal_execution_text(), _tp.kb_terminal_back())
    elif data == "terminal:risk":
        await edit(
            _terminal_risk_text(await _tg_backend.control_runtime_snapshot(engine, _settings, priority="normal")),
            InlineKeyboardMarkup([[InlineKeyboardButton("⚙️ Risk settings", callback_data="s:risk_cfg")], [InlineKeyboardButton("🏠 Home", callback_data="main_menu")]]),
        )
    elif data == "terminal:exits":
        await edit(
            "\n".join([
                "*🔴 LIVE · EXITS*", "",
                f"Trailing: `{'ON' if _settings.get_val('trailing_enabled', True) else 'OFF'}`",
                f"Break-even: `{'ON' if _settings.get_val('breakeven_enabled', True) else 'OFF'}`",
                f"Profit lock: `{'ON' if _settings.get_val('profit_lock_enabled', True) else 'OFF'}`",
                f"Partial TP: `{'ON' if _settings.get_val('partial_tp_enabled', True) else 'OFF'}`",
                "", "Exchange protection remains authoritative for live positions.",
            ]),
            InlineKeyboardMarkup([[InlineKeyboardButton("🎯 Exit settings", callback_data="set:fg:trailing")], [InlineKeyboardButton("🏠 Home", callback_data="main_menu")]]),
        )
    elif data == "terminal:liquidity":
        await edit(
            "\n".join([
                "*🔴 LIVE · LIQUIDITY*", "",
                f"Max economic spread: `{float(_settings.get_val('execution_max_economic_spread_pct', 0.30) or 0.30):.3f}%`",
                f"Max slippage: `{float(_settings.get_val('execution_max_slippage_pct', 0.35) or 0.35):.3f}%`",
                f"Participation cap: `${float(_settings.get_val('participation_cap_usdt', 0.0) or 0.0):.2f}`",
                "", "Live order economics are revalidated immediately before submission.",
            ]),
            InlineKeyboardMarkup([[InlineKeyboardButton("⚙️ Execution settings", callback_data="s:execution")], [InlineKeyboardButton("🏠 Home", callback_data="main_menu")]]),
        )
    elif data == "terminal:performance":
        await edit(_production_pnl_text(await _tg_backend.control_pnl_snapshot(priority="normal"), await _live_position_snapshot()), _tp.kb_terminal_back())
    elif data == "terminal:research":
        import production_research_funnel as _terminal_funnel
        row = _terminal_funnel.latest()
        await edit(
            "\n".join([
                "*🔴 LIVE · RESEARCH*", "",
                "Counterfactual evidence is isolated from live PnL.",
                f"Last scan: `{row.get('finished_at', row.get('timestamp', 'not available'))}`",
                f"Regime: `{str((row.get('regime', {}) or {}).get('regime', 'UNKNOWN')).upper()}`",
                f"Symbols ready: `{int(row.get('symbols_data_ready', 0) or 0)}/{int(row.get('symbols_seen', 0) or 0)}`",
                "", "This room renders the latest precomputed snapshot.",
            ]),
            _tp.kb_terminal_back(),
        )
    elif data == "terminal:system":
        await edit(
            _terminal_system_text(await _tg_backend.control_runtime_snapshot(engine, _settings, priority="normal"), _tg_backend.telegram_health_snapshot()),
            _tp.kb_terminal_back(),
        )
    elif data == "terminal:why_no_trade":
        await edit(_terminal_why_no_trade_text(), _tp.kb_terminal_back())
    elif data == "terminal:recent_events":
        from execution_flow_telemetry import snapshot as _terminal_flow_snapshot
        flow = _terminal_flow_snapshot(15)
        lines = ["*🔴 LIVE · RECENT EXECUTION EVENTS*", ""]
        for row in flow.get("recent", []):
            lines.append(f"`{row.get('timestamp', '')[11:19]}` `{row.get('coin', '?')}` `{row.get('stage', '?')}` · `{row.get('reason', '') or 'OK'}`")
        if len(lines) == 2:
            lines.append("No production execution events recorded.")
        await edit("\n".join(lines), _tp.kb_terminal_back())
    elif data == "terminal:emergency":
        await edit(
            "*🚨 LIVE EMERGENCY CONTROLS*\n\nDISARM blocks new entries. HALT keeps position protection and reconciliation active. Emergency close requires separate confirmation.",
            InlineKeyboardMarkup([
                [InlineKeyboardButton("🚨 DISARM NEW ENTRIES", callback_data="live:disarm")],
                [InlineKeyboardButton("🛑 HALT ADMISSIONS", callback_data="live:halt_confirm")],
                [InlineKeyboardButton("⚠️ EMERGENCY CLOSE", callback_data="live:emergency_confirm")],
                [InlineKeyboardButton("🏠 Home", callback_data="main_menu")],
            ]),
        )
    elif data in {"s:modes", "diag:ai", "set:cat:ai", "set:fg:ai_gate", "aiscan_quick", "chat", "ai_confirm", "ai_cancel", "ai_adjust"}:
        await edit("*This control has moved.*\n\nUse the production terminal Home menu.", _tp.kb_home(engine.running))
    elif data == "home_control":
        counts = _engine_mode_breakdown()
        snap = engine.status_snapshot()
        ws = "📡 WS" if snap.get("ws_active") else "🔄 REST"
        live = (await _tg_backend.control_runtime_snapshot(engine, _settings, priority="high")).get("live_state", {})
        kb = InlineKeyboardMarkup([
            [
                InlineKeyboardButton(
                    "🚨 DISARM NOW" if live.get("armed") else "✅ ARM",
                    callback_data="live:disarm" if live.get("armed") else "live:arm_confirm",
                ),
                InlineKeyboardButton("Status", callback_data="home_status"),
            ],
            [
                InlineKeyboardButton("Positions", callback_data="positions_view"),
                InlineKeyboardButton("Sync", callback_data="sync_positions"),
            ],
            [InlineKeyboardButton("✕ Close All", callback_data="close_all_confirm")],
            [InlineKeyboardButton("← Home", callback_data="main_menu")],
        ])
        await edit(
            (
                f"🎛 *{_live_state_badge(live)} · CONTROL*\n\n"
                f"Market data: `{ws}`\n"
                f"Engines: `{engine.running_count()}` running · `{counts['live']}` live · `{counts['shadow']}` shadow\n"
                "DISARM blocks new entries while exits and reconciliation continue."
            ),
            kb,
        )
    elif data == "home_status":
        runtime = await _tg_backend.control_runtime_snapshot(engine, _settings, priority="high")
        account, live_snapshot = await _tg_backend.fast_operational_exchange_snapshot(
            okx,
            _live_position_snapshot,
        )
        await edit(
            _production_status_text(
                runtime,
                account,
                live_snapshot,
                await _tg_backend.control_pnl_snapshot(priority="normal"),
                _tg_backend.telegram_health_snapshot(),
            ),
            InlineKeyboardMarkup([[InlineKeyboardButton("← Home", callback_data="main_menu")]]),
        )
    elif data == "engines_menu":
        try: bal = await okx.get_usdt_balance()
        except: bal = 0.0
        await edit(f"⚡ *Engines*\n\nBalance: `${bal:,.4f}`\nTap engine for controls.", engines_menu())
    elif data == "eng_start_all":
        msg = await engine.start_all()
        await edit(msg, _tp.kb_home(engine.running))
    elif data == "eng_stop_all":
        msg = await engine.stop_all()
        await edit(msg, _tp.kb_home(engine.running))
    elif data.startswith("engine_detail_"):
        name = data.replace("engine_detail_","")
        await edit(_engine_text(name), engine_detail_kb(name))
    elif data.startswith("eng_toggle_"):
        name = data.replace("eng_toggle_",""); eng2 = engine.engines.get(name)
        if eng2:
            if eng2.running: await engine.stop_engine(name)
            else: await engine.start_engine(name)
        await edit(_engine_text(name), engine_detail_kb(name))
    elif data.startswith("eng_enable_"):
        name = data.replace("eng_enable_",""); new = _settings.toggle_engine(name)
        await q.answer(f"{name} {'enabled ✅' if new else 'disabled ⚫'}")
        await edit(_engine_text(name), engine_detail_kb(name))
    elif data.startswith("eng_mode_cycle_"):
        name = data.replace("eng_mode_cycle_","").upper()
        # Cycle: disabled → shadow → probation → live → disabled
        _mode_cycle = ["disabled", "shadow", "probation", "live"]
        cur = _settings.get_engine_mode(name)
        idx = _mode_cycle.index(cur) if cur in _mode_cycle else 0
        new_mode = _mode_cycle[(idx + 1) % len(_mode_cycle)]
        _settings.set_engine_mode(name, new_mode, actor="telegram_operator", reason="manual mode cycle")
        await q.answer(f"{name} → {new_mode.upper()}")
        await edit(_engine_text(name), engine_detail_kb(name))
    elif data.startswith("eng_closeall_"):
        await edit(await engine.close_engine_positions(data.replace("eng_closeall_","")), engines_menu())
    elif data == "positions_view":
        positions = {p["coin"]: p for p in engine.all_open_positions()}
        _account, live_snapshot = await _tg_backend.fast_operational_exchange_snapshot(
            okx,
            lambda: _live_position_snapshot(force_refresh=True),
        )
        await edit(_positions_text(positions, live_snapshot), positions_kb(positions, live_snapshot))
    elif data == "sync_positions":
        try:
            result = await engine.sync_positions(repair=True)
        except Exception as e:
            result = f"⚠️ Sync failed: `{type(e).__name__}: {e}`"
        await edit(
            f"🔄 *Registry Sync*\n\n{result}",
            InlineKeyboardMarkup([[InlineKeyboardButton("← Control", callback_data="home_control"),
                                   InlineKeyboardButton("← Home", callback_data="main_menu")]]),
        )
    elif data.startswith("close_coin_"):
        coin = data.replace("close_coin_","")
        # Acknowledge the Telegram callback before P0 truth/metadata work;
        # the durable close result is rendered separately below.
        await q.answer("CLOSE_REQUEST_ACCEPTED")
        r = await engine.close_coin(coin)
        positions = {p["coin"]: p for p in engine.all_open_positions()}
        live_snapshot = await _live_position_snapshot(force_refresh=True)
        await edit(f"{r}\n\n" + _positions_text(positions, live_snapshot), positions_kb(positions, live_snapshot))
    elif data == "close_all_confirm":
        await q.answer("CLOSE_REQUEST_ACCEPTED")
        await edit(await engine.close_all_positions(), main_menu())
    elif data == "reports_menu":
        await edit("📈 *Reports*", reports_menu())
    elif data == "report_combined":
        await edit(_production_pnl_text(await _tg_backend.control_pnl_snapshot(priority="normal"), await _live_position_snapshot()), reports_menu())
    elif data == "report_winrate":
        await edit(_production_pnl_text(await _tg_backend.control_pnl_snapshot(priority="normal"), await _live_position_snapshot()), reports_menu())
    elif data in ("report_trades", "trades_view"):
        await edit(_production_trades_text(await _tg_backend.control_pnl_snapshot(priority="normal")), _tp.kb_trades())
    elif data.startswith("report_"):
        name = data.replace("report_","").upper()
        if name in ENGINE_NAMES: await edit(format_engine_report(name), reports_menu())
    elif data in ("portfolio", "p:portfolio"):
        live_snapshot = await _live_position_snapshot()
        account = await _tg_backend.account_snapshot(okx, position_snapshot=live_snapshot)
        await edit(
            _production_balance_text(account, await _tg_backend.control_runtime_snapshot(engine, _settings, priority="high"))
            + "\n\n"
            + _production_pnl_text(await _tg_backend.control_pnl_snapshot(priority="normal"), live_snapshot),
            _tp.kb_portfolio(),
        )
    elif data == "aiscan_quick":
        await edit("🧠 *AI scan removed*\n\nUse the live diagnostics and engine panels instead.", main_menu())

    elif data == "universe_quick":
        try:
            uni = await get_futures_universe()
            await edit(f"🌐 *Universe* — {len(uni)} coins\n\n`" + "  ".join(uni[:30]) + "`",
                       InlineKeyboardMarkup([[InlineKeyboardButton("← Home", callback_data="main_menu")]]))
        except Exception as e: await edit(f"⚠️ {e}", main_menu())
    elif data == "chat":
        chat_mode.pop(uid, None)
        await edit("Plain text control has been removed. Use the live menu controls instead.", main_menu())

    # AI proposal flow removed.
    elif data == "ai_confirm":
        await edit("🤖 AI proposal flow removed.", main_menu())
    elif data == "ai_cancel":
        await edit("🤖 AI proposal flow removed.", main_menu())
    elif data == "ai_adjust":
        await edit("🤖 AI proposal flow removed.", main_menu())
    elif data == "help_menu":
        await edit(_tp.HELP_MAIN_TEXT, help_menu())
    elif data in ("help_commands", "help:commands"):
        await edit(_tp.HELP_COMMANDS_TEXT,
                   InlineKeyboardMarkup([[InlineKeyboardButton("← Help", callback_data="help_menu")]]))
    elif data == "help_shogun":
        kz = killzone_status()
        await edit(f"🏯 *SHOGUN — CRT/SMC/ICT*\n\n_Institutional price action engine._\n\n*Votes needed (2 of 5):*\n  • CRT Sweep — 4H liquidity grab + reversal\n  • Fair Value Gap — 3-candle imbalance\n  • Order Block — institutional OB retest\n  • CHoCH/BOS — structure shift\n  • L/S Ratio — retail crowd contrarian\n\n*Kill Zone filter:*\n  🇬🇧 London 02:00–05:00 UTC\n  🇺🇸 New York 13:30–16:30 UTC\n\n  Now: _{kz}_",
                   InlineKeyboardMarkup([[InlineKeyboardButton("← Help", callback_data="help_menu")]]))
    elif data == "help_susanoo":
        thresh = float(_v('susanoo_funding_threshold', 0.0005)) * 100
        await edit(f"⛩️ *SUSANOO — Funding Rate Squeeze*\n\n_Fades over-extended perpetual positions using funding rate extremes._\n\n*Votes needed (2 of 3):*\n  • Funding ≥ `{thresh:.3f}%` → SHORT (longs crowded)\n  • Funding ≤ `-{thresh:.3f}%` → LONG (shorts crowded)\n  • Price within 0.5% of 20-bar S/R\n  • Volume ≥ 1.5× 20-bar average\n\nLeverage: `{_engine_target_leverage()}x+` · SL: `1.5%` fixed · TP2: `1:5 RRR`\n\n_When longs pay shorts, the long side is crowded. SUSANOO fades them._",
                    InlineKeyboardMarkup([[InlineKeyboardButton("← Help", callback_data="help_menu")]]))
    elif data == "help_fujin":
        await edit("💨 *FUJIN — Order Flow Imbalance*\n\n_Uses OKX taker buy/sell volume to detect institutional order flow before price reacts._\n\n*Votes needed (2 of 3):*\n  • Taker buy ratio ≥ 65% → LONG\n  • Taker buy ratio ≤ 35% → SHORT\n  • RSI 40–60 (mid-zone, not stretched)\n  • Price above/below EMA21\n\n_Taker volume is real OKX data — not price-derived. When buyers dominate and price hasn't moved yet, FUJIN enters early._",
                   InlineKeyboardMarkup([[InlineKeyboardButton("← Help", callback_data="help_menu")]]))
    elif data == "help_amaterasu":
        lev = int(_v('amaterasu_leverage', 15))
        await edit(f"☀️ *AMATERASU — Multi-Timeframe Confluence*\n\n_Only trades when 1m + 15m + 1H all agree. Highest-conviction filter in the bot._\n\n*Votes needed (3 of 3 — all must agree):*\n  • 1m:  EMA21 > EMA55 → LONG\n  • 15m: EMA21 > EMA55 → LONG\n  • 1H:  Price > EMA50  → LONG\n\n*RSI filter:* skips overbought (>72) or oversold (<28) on 1H.\n\nLeverage: `{lev}x` · SL: structure-based · TP2: 1:5 RRR",
                   InlineKeyboardMarkup([[InlineKeyboardButton("← Help", callback_data="help_menu")]]))
    elif data == "help_orochi":
        pb = float(_v('orochi_pullback_buffer_pct', 0.45))
        vol = float(_v('orochi_volume_confirm_mult', 1.1))
        crowd = float(_v('orochi_funding_abs_max', 0.0008)) * 100
        lev = int(_v('orochi_leverage', 15))
        await edit(
            f"🐍 *OROCHI — Liquid Trend Pullback*\n\n"
            f"_Large-cap continuation engine. It trades only when higher-timeframe trend, a clean 15m pullback reclaim, and a 1m trigger all align._\n\n"
            f"*Votes needed (3 of 3):*\n"
            f"  ✅ 4H trend bias aligned with EMA50/EMA200 structure\n"
            f"  ✅ 15m pullback holds within `{pb:.2f}%` of trend value and reclaims EMA21/VWAP\n"
            f"  ✅ 1m trigger confirms with EMA8/EMA21, VWAP, momentum, and volume ≥ `{vol:.2f}x`\n"
            f"  ✅ Funding is only a crowding veto when |rate| exceeds `{crowd:.3f}%`\n\n"
            f"Universe: `Top-20 large caps only` · Leverage: `{lev}x` · NANO routing: `off`\n\n"
            f"_OROCHI no longer fades crowded markets. It now joins strong liquid trends after controlled pullbacks._",
            InlineKeyboardMarkup([[InlineKeyboardButton("← Help", callback_data="help_menu")]]),
        )
    elif data == "help_tsunami":
        liq_thresh = float(_v('tsunami_liq_threshold_usd', 50000))
        await edit(f"🌊 *TSUNAMI — Liquidation Cascade*\n\n_Detects large liquidation clusters and rides the cascade momentum._\n\n*Votes needed (2 of 3):*\n  ✅ Liq cluster ≥ `${liq_thresh:,.0f}` in 5 min\n  ✅ Price approaching cluster level\n  ✅ Volume confirming directional pressure\n  ✅ RSI not exhausted in trade direction\n\nLeverage: `{_engine_target_leverage()}x+` · SL: `1.0%` tight (fast ride) · TP2: `1:4 RRR`\n\n_When a large cluster gets hit, cascading stops accelerate price. TSUNAMI enters with the wave._",
                    InlineKeyboardMarkup([[InlineKeyboardButton("🏠 Help", callback_data="help_menu")]]))
    elif data == "help_suijin":
        band = float(_v('suijin_vwap_deviation_pct', 1.5))
        await edit(f"💧 *SUIJIN — VWAP Reversion*\n\n_Fades price deviations from VWAP when stretched beyond normal bands._\n\n*Votes needed (2 of 3):*\n  ✅ Price ≥ `{band:.1f}%` above VWAP → SHORT\n  ✅ Price ≥ `{band:.1f}%` below VWAP → LONG\n  ✅ RSI confirming over-extension (>65 or <35)\n  ✅ Volume below average (no momentum)\n\nLeverage: `{_engine_target_leverage()}x+` · TP target: VWAP mean · TP2: min `1:4 RRR`\n\n_RANGING regime only — engine auto-pauses during trending markets._",
                    InlineKeyboardMarkup([[InlineKeyboardButton("🏠 Help", callback_data="help_menu")]]))
    elif data == "help_raijuu":
        oi_change = float(_v('raijuu_oi_change_threshold', 0.05)) * 100
        await edit(f"⚡ *RAIJUU — OI Divergence*\n\n_Trades when Open Interest and price move in opposite directions — a leading signal of reversal._\n\n*Votes needed (2 of 3):*\n  ✅ Price rising + OI falling → SHORT (weak rally)\n  ✅ Price falling + OI falling → LONG (short covering)\n  ✅ OI change ≥ `{oi_change:.1f}%` required\n  ✅ Funding rate neutral (not already extreme)\n\nLeverage: `{_engine_target_leverage()}x+` · SL: structure-based · TP2: `1:5 RRR` min `1:4`\n\n_OI divergence precedes reversals. RAIJUU catches turns before price confirms._",
                    InlineKeyboardMarkup([[InlineKeyboardButton("🏠 Help", callback_data="help_menu")]]))
    elif data in ("help_risk", "help:risk"):
        await edit(_tp.HELP_RISK_TEXT,
                   InlineKeyboardMarkup([[InlineKeyboardButton("← Help", callback_data="help_menu")]]))
    elif data == "v6_status":
        await q.answer()
        try:
            try:
                bal = await okx.get_usdt_balance()
            except Exception:
                bal = 0.0
            lines = ["⚙️ *OxyPulse V6.1 Status*\n"]
            v6_on = bool(_v("v6_enabled", False))
            lines.append(f"*V6 master gate:* `{'ON ✅' if v6_on else 'OFF ❌'}`")
            if _v("v6_capital_adaptive_enabled", True):
                from capital_manager import get_profile as _cm_get
                _prof = _cm_get(bal)
                mi = {"MICRO": "🟢", "GROWTH": "🔵", "PRO": "🟣"}.get(_prof.mode, "⚪")
                lines.append(
                    f"\n*Capital Mode:* {mi} `{_prof.mode}`\n"
                    f"  Balance: `${bal:,.2f}` · Size×: `{_prof.size_multiplier:.2f}` · "
                    f"DD tol: `{_prof.drawdown_tolerance_pct:.1f}%` · "
                    f"Max/hr: `{_prof.max_trades_per_hour}`"
                )
            if _v("v6_anti_drawdown_enabled", False):
                from drawdown_guard import get_drawdown_state as _dd_get
                try:
                    from capital_manager import get_profile as _cm_g2
                    _pr2 = _cm_g2(bal)
                except Exception:
                    _pr2 = None
                _dd = _dd_get(capital_profile=_pr2)
                di = {0: "🟢", 1: "🟡", 2: "🟠", 3: "🔴", 4: "🛑"}.get(_dd.level, "⚪")
                lines.append(
                    f"\n*Drawdown Guard:* {di} L`{_dd.level}` `{_dd.label}` · "
                    f"Daily `{_dd.daily_dd_pct:.1f}%` · Size× `{_dd.size_multiplier:.2f}`"
                )
            if _v("v6_correlation_control_enabled", True):
                from exposure_tracker import get_exposure as _exp_get
                _exp = _exp_get(bal)
                lines.append(
                    f"\n*Exposure:* Long `${_exp.long_usdt:,.0f}` ({_exp.long_count}) · "
                    f"Short `${_exp.short_usdt:,.0f}` ({_exp.short_count}) · "
                    f"Net `{_exp.net_exposure_pct:.1f}%`"
                )
            flag_map = {
                "v6_weighted_confidence_enabled": "Confidence",
                "dynamic_sizing_enabled":         "Dyn Sizing",
                "v6_anti_drawdown_enabled":       "DD Guard",
                "v6_smart_execution_enabled":     "Exec Router",
            "engine_weight_adaptation_enabled": "Eng Adapt",
                "v6_market_filter_enabled":       "Mkt Filter",
                "v6_trade_frequency_enabled":     "Freq Ctrl",
            }
            flags = "  ".join(f"{'✅' if _v(k) else '❌'} {lbl}" for k, lbl in flag_map.items())
            lines.append(f"\n*Flags:*\n  {flags}")
            v6_kb = InlineKeyboardMarkup([[InlineKeyboardButton("← Settings", callback_data="settings_menu")]])
            await edit("\n".join(lines), v6_kb)
        except Exception as e:
            await edit(f"⚠️ V6.1 status error: {e}", settings_menu())
        return
    elif data == "settings_menu":
        await edit(_tp.panel_settings_hub_text(_settings), _tp.kb_settings_hub(_settings))
    elif data in {"s:ai", "diag:ai"}:
        await edit(
            "Removed module\n\nUse Diagnostics, Strategy, and Engine panels for current runtime visibility.",
            InlineKeyboardMarkup([[InlineKeyboardButton("Back", callback_data="s:diagnostics" if data == "diag:ai" else "settings_menu")]]),
        )

    # ── Settings Category Hub (category-first) ───────────────────────────────
    elif data == "set:cat:trading":
        await edit(_tp.panel_settings_cat_trading_text(_settings), _tp.kb_settings_cat_trading(_settings))
    elif data == "set:cat:strategy":
        await edit(_tp.panel_settings_cat_strategy_text(_settings), _tp.kb_settings_cat_strategy(_settings))
    elif data == "set:cat:risk":
        await edit(_tp.panel_settings_cat_risk_text(_settings), _tp.kb_settings_cat_risk(_settings))
    elif data == "set:cat:execution":
        await edit(_tp.panel_settings_cat_execution_text(_settings), _tp.kb_settings_cat_execution(_settings))
    elif data == "set:cat:capital":
        await edit(_tp.panel_settings_cat_capital_text(_settings), _tp.kb_settings_cat_capital(_settings))
    elif data == "set:cat:ai":
        await edit(_tp.panel_settings_cat_system_text(_settings), _tp.kb_settings_cat_system(_settings))
    elif data == "set:cat:diagnostics":
        await edit(_tp.panel_settings_cat_diagnostics_text(_settings), _tp.kb_settings_cat_diagnostics(_settings))
    elif data == "set:cat:system":
        await edit(_tp.panel_settings_cat_system_text(_settings), _tp.kb_settings_cat_system(_settings))
    elif data == "set:cat:participation":
        await edit(_tp.panel_settings_participation_text(_settings), _tp.kb_settings_participation(_settings))

    # ── Feature Groups (Level 3) ────────────────────────────────────────────
    elif data == "set:fg:slots":
        await edit(_tp.panel_fg_slots_text(_settings), _tp.kb_fg_slots(_settings))
    elif data == "set:fg:engine_caps":
        await edit(_tp.panel_fg_engine_caps_text(_settings), _tp.kb_fg_engine_caps(_settings))
    elif data == "set:fg:confidence":
        await edit(_tp.panel_fg_confidence_text(_settings), _tp.kb_fg_confidence(_settings))
    elif data == "set:fg:weekly_cap":
        await edit(_tp.panel_fg_weekly_cap_text(_settings), _tp.kb_fg_weekly_cap(_settings))
    elif data == "set:fg:trailing":
        await edit(_tp.panel_fg_trailing_text(_settings), _tp.kb_fg_trailing(_settings))
    elif data == "set:fg:breakeven":
        await edit(_tp.panel_fg_breakeven_text(_settings), _tp.kb_fg_breakeven(_settings))
    elif data == "set:fg:partials":
        await edit(_tp.panel_fg_partials_text(_settings), _tp.kb_fg_partials(_settings))
    elif data == "set:fg:profit_lock":
        await edit(_tp.panel_fg_profit_lock_text(_settings), _tp.kb_fg_profit_lock(_settings))
    elif data == "set:fg:early_invalidation":
        await edit(_tp.panel_fg_early_invalidation_text(_settings), _tp.kb_fg_early_invalidation(_settings))
    elif data == "set:fg:repeat_loss_cooling":
        await edit(_tp.panel_fg_repeat_loss_cooling_text(_settings), _tp.kb_fg_repeat_loss_cooling(_settings))
    elif data == "set:fg:deployment":
        await edit(_tp.panel_fg_deployment_text(_settings), _tp.kb_fg_deployment(_settings))
    elif data == "set:fg:size":
        await edit(_tp.panel_fg_size_text(_settings), _tp.kb_fg_size(_settings))
    elif data == "set:fg:ai_gate":
        await edit(_tp.panel_settings_hub_text(_settings), _tp.kb_settings_hub(_settings))
    elif data == "set:fg:exposure":
        await edit(_tp.panel_fg_exposure_text(_settings), _tp.kb_fg_exposure(_settings))
    elif data == "set:fg:risk_ceiling":
        await edit(_tp.panel_fg_risk_ceiling_text(_settings), _tp.kb_fg_risk_ceiling(_settings))
    elif data == "set:fg:short_rehab":
        await edit(_tp.panel_fg_short_rehab_text(_settings), _tp.kb_fg_short_rehab(_settings))
    elif data == "set:fg:amaterasu_macro":
        await edit(_tp.panel_fg_amaterasu_macro_text(_settings), _tp.kb_fg_amaterasu_macro(_settings))

    # ── Weekly Cap Rooms (one-setting-per-room) ─────────────────────────────
    elif data == "wcap:enable":
        await edit(_tp.panel_wcap_enable_text(_settings), _tp.kb_wcap_enable(_settings))
    elif data in {"wcap:enable:on", "wcap:enable:off"}:
        _settings.set_val("weekly_cap_enabled", data.endswith(":on"))
        await edit(_tp.panel_wcap_enable_text(_settings), _tp.kb_wcap_enable(_settings))

    elif data == "wcap:hub":
        _bal = await _settings_balance()
        await edit(_tp.panel_weekly_cap_hub_text(_settings, balance=_bal), _tp.kb_weekly_cap_hub(_settings))
    elif data == "wcap:enabled:toggle":
        _settings.set_val("weekly_cap_enabled", not bool(_v("weekly_cap_enabled", True)))
        _bal = await _settings_balance()
        await edit(_tp.panel_weekly_cap_hub_text(_settings, balance=_bal), _tp.kb_weekly_cap_hub(_settings))
    elif data == "wcap:mode":
        await edit(_tp.panel_weekly_cap_mode_text(_settings), _tp.kb_weekly_cap_mode(_settings))
    elif data in {"wcap:mode:off", "wcap:mode:soft", "wcap:mode:strict", "wcap:mode:adaptive"}:
        mode = str(data).split(":")[-1]
        _settings.set_val("weekly_cap_mode", mode)
        await edit(_tp.panel_weekly_cap_mode_text(_settings), _tp.kb_weekly_cap_mode(_settings))
    elif data == "wcap:threshold":
        await edit(_tp.panel_wcap_threshold_text(_settings), _tp.kb_wcap_threshold(_settings))
    elif data in {"wcap:threshold:up", "wcap:threshold:dn"}:
        thr = float(_v("weekly_cap_threshold_pct", 25.0) or 25.0)
        step = 1.0
        thr = thr + step if data.endswith(":up") else thr - step
        thr = max(min(thr, 95.0), 0.0)
        # Keep legacy sync key stable as well.
        _settings.set_val("weekly_cap_threshold_pct", round(thr, 2))
        await edit(_tp.panel_wcap_threshold_text(_settings), _tp.kb_wcap_threshold(_settings))

    elif data == "wcap:recovery":
        await edit(_tp.panel_wcap_recovery_text(_settings), _tp.kb_wcap_recovery(_settings))
    elif data in {"wcap:recovery:up", "wcap:recovery:dn"}:
        rec = float(_v("weekly_cap_recovery_threshold_pct", 15.0) or 15.0)
        thr = float(_v("weekly_cap_threshold_pct", 25.0) or 25.0)
        step = 1.0
        rec = rec + step if data.endswith(":up") else rec - step
        rec = max(min(rec, thr), 0.0) if thr > 0 else max(rec, 0.0)
        _settings.set_val("weekly_cap_recovery_threshold_pct", round(rec, 2))
        await edit(_tp.panel_wcap_recovery_text(_settings), _tp.kb_wcap_recovery(_settings))

    elif data == "wcap:minconf":
        await edit(_tp.panel_wcap_minconf_text(_settings), _tp.kb_wcap_minconf(_settings))
    elif data in {"wcap:minconf:up", "wcap:minconf:dn"}:
        conf = float(_v("weekly_cap_min_confidence", 4.2) or 4.2)
        step = 0.1
        conf = conf + step if data.endswith(":up") else conf - step
        conf = max(min(conf, 10.0), 0.0)
        _settings.set_val("weekly_cap_min_confidence", round(conf, 2))
        await edit(_tp.panel_wcap_minconf_text(_settings), _tp.kb_wcap_minconf(_settings))

    elif data == "wcap:hqonly":
        await edit(_tp.panel_wcap_hqonly_text(_settings), _tp.kb_wcap_hqonly(_settings))
    elif data in {"wcap:hqonly:on", "wcap:hqonly:off"}:
        _settings.set_val("weekly_cap_allow_high_conf_only", data.endswith(":on"))
        await edit(_tp.panel_wcap_hqonly_text(_settings), _tp.kb_wcap_hqonly(_settings))

    elif data == "wcap:reduction":
        await edit(_tp.panel_wcap_reduction_text(_settings), _tp.kb_wcap_reduction(_settings))
    elif data in {"wcap:reduction:up", "wcap:reduction:dn"}:
        red = float(_v("weekly_cap_size_reduction", 0.5) or 0.5)
        step = 0.05
        red = red + step if data.endswith(":up") else red - step
        red = max(min(red, 1.0), 0.05)
        _settings.set_val("weekly_cap_size_reduction", round(red, 3))
        await edit(_tp.panel_wcap_reduction_text(_settings), _tp.kb_wcap_reduction(_settings))

    elif data == "wcap:cooldown":
        await edit(_tp.panel_wcap_cooldown_text(_settings), _tp.kb_wcap_cooldown(_settings))
    elif data in {"wcap:cooldown:up", "wcap:cooldown:dn"}:
        cd = float(_v("weekly_cap_cooldown_seconds", 120.0) or 120.0)
        step = 30.0
        cd = cd + step if data.endswith(":up") else cd - step
        cd = max(min(cd, 3600.0), 0.0)
        _settings.set_val("weekly_cap_cooldown_seconds", int(cd))
        await edit(_tp.panel_wcap_cooldown_text(_settings), _tp.kb_wcap_cooldown(_settings))

    # Legacy weekly-cap pages (kept for backward compatibility).
    elif data == "wcap:thresholds":
        await edit(_tp.panel_weekly_cap_thresholds_text(_settings), _tp.kb_weekly_cap_thresholds(_settings))
    elif data in {"wcap:thr:up", "wcap:thr:dn", "wcap:rec:up", "wcap:rec:dn"}:
        thr = float(_v("weekly_cap_threshold_pct", 25.0) or 25.0)
        rec = float(_v("weekly_cap_recovery_threshold_pct", 15.0) or 15.0)
        step = 1.0
        if data == "wcap:thr:up":
            thr = min(thr + step, 95.0)
        elif data == "wcap:thr:dn":
            thr = max(thr - step, 0.0)
        elif data == "wcap:rec:up":
            rec = rec + step
        elif data == "wcap:rec:dn":
            rec = rec - step
        rec = max(min(rec, thr), 0.0) if thr > 0 else max(rec, 0.0)
        _settings.set_val("weekly_cap_threshold_pct", round(thr, 2))
        _settings.set_val("weekly_cap_recovery_threshold_pct", round(rec, 2))
        await edit(_tp.panel_weekly_cap_thresholds_text(_settings), _tp.kb_weekly_cap_thresholds(_settings))
    elif data == "wcap:quality":
        await edit(_tp.panel_weekly_cap_quality_text(_settings), _tp.kb_weekly_cap_quality(_settings))
    elif data == "wcap:hqonly:toggle":
        _settings.set_val("weekly_cap_allow_high_conf_only", not bool(_v("weekly_cap_allow_high_conf_only", True)))
        await edit(_tp.panel_weekly_cap_quality_text(_settings), _tp.kb_weekly_cap_quality(_settings))
    elif data in {"wcap:red:up", "wcap:red:dn", "wcap:cd:up", "wcap:cd:dn"}:
        red = float(_v("weekly_cap_size_reduction", 0.5) or 0.5)
        cd = float(_v("weekly_cap_cooldown_seconds", 120.0) or 120.0)
        if data == "wcap:red:up":
            red = min(red + 0.05, 1.0)
        elif data == "wcap:red:dn":
            red = max(red - 0.05, 0.05)
        elif data == "wcap:cd:up":
            cd = min(cd + 30.0, 3600.0)
        elif data == "wcap:cd:dn":
            cd = max(cd - 30.0, 0.0)
        _settings.set_val("weekly_cap_size_reduction", round(red, 3))
        _settings.set_val("weekly_cap_cooldown_seconds", int(cd))
        await edit(_tp.panel_weekly_cap_reduction_text(_settings), _tp.kb_weekly_cap_reduction(_settings))

    # ── Room Renderers (Level 4) ────────────────────────────────────────────
    elif data == "set:room:global_slot_mode":
        await edit(_tp.panel_room_global_slot_mode(_settings), _tp.kb_room_global_slot_mode(_settings))
    elif data == "set:room:max_open_positions":
        await edit(_tp.panel_room_max_open_positions(_settings), _tp.kb_room_max_open_positions(_settings))
    elif data == "set:room:trade_selection_policy":
        await edit(_tp.panel_room_trade_selection_policy(_settings), _tp.kb_room_trade_selection_policy(_settings))
    elif data.startswith("set:room:engine_caps:"):
        try:
            page = int(str(data).split(":")[-1])
        except Exception:
            page = 0
        await edit(_tp.panel_room_engine_caps_list_text(_settings, page=page), _tp.kb_room_engine_caps_list(_settings, page=page))
    elif data == "set:room:tp_mode":
        await edit(_tp.panel_room_tp_mode(_settings), _tp.kb_room_tp_mode(_settings))
    elif data == "set:room:size_mode":
        await edit(_tp.panel_room_size_mode(_settings), _tp.kb_room_size_mode(_settings))

    # Generic boolean rooms.
    elif data in {
        "set:room:tsl_enabled",
        "set:room:breakeven_enabled",
        "set:room:partial_exit_enabled",
        "set:room:profit_lock_enabled",
        "set:room:early_invalidation_enabled",
        "set:room:adaptive_early_invalidation_enabled",
        "set:room:profitability_telemetry_enabled",
        "set:room:early_invalidation_near_tp_grace_enabled",
        "set:room:symbol_direction_loss_cooldown_enabled",
        "set:room:realized_risk_ceiling_enabled",
        "set:room:realized_risk_ceiling_tighten_sl_if_needed",
        "set:room:realized_risk_ceiling_telemetry_enabled",
        "set:room:short_rehab_enabled",
        "set:room:fee_aware_partial_deferral_enabled",
        "set:room:continuation_runner_enabled",
        "set:room:exhaustion_assisted_tightening_enabled",
        "set:room:dynamic_sizing_enabled",
        "set:room:v6_telemetry_enabled",
        "set:room:block_diagnostics_enabled",
        "set:room:engine_weight_adaptation_enabled",
    }:
        key = str(data).split(":")[-1]
        await _render_bool_room(key)

    # Generic numeric rooms.
    elif data in {
        "set:room:trailing_activation_pct",
        "set:room:trailing_distance_pct",
        "set:room:trailing_after_tp1_pct",
        "set:room:trailing_after_tp2_pct",
        "set:room:runner_core_min_remaining_pct",
        "set:room:continuation_runner_core_min_pct",
        "set:room:continuation_runner_min_roi",
        "set:room:continuation_runner_score_threshold",
        "set:room:continuation_runner_tp2_close_mult",
        "set:room:continuation_runner_trail_mult",
        "set:room:exhaustion_tighten_score_threshold",
        "set:room:exhaustion_tighten_trail_mult",
        "set:room:position_size_usdt",
        "set:room:position_size_pct",
        "set:room:risk_pct",
        "set:room:realized_risk_cap_pct_balance",
        "set:room:realized_risk_cap_min_usdt",
        "set:room:realized_risk_cap_max_usdt",
        "set:room:short_rehab_weak_regime_size_mult",
        "set:room:short_rehab_min_adx_1h",
        "set:room:v6_min_position_size_usdt",
        "set:room:v6_max_position_size_usdt",
        "set:room:v6_max_position_pct_of_balance",
        "set:room:breakeven_trigger_pct",
        "set:room:breakeven_activation_profit_usd",
        "set:room:breakeven_buffer_pct",
        "set:room:tp1_trigger_pct",
        "set:room:partial_tp1_close_pct",
        "set:room:tp2_trigger_pct",
        "set:room:partial_tp2_close_pct",
        "set:room:min_net_take_profit_usdt",
        "set:room:min_full_close_profit_usdt",
        "set:room:minimum_net_capture_fee_multiple",
        "set:room:partial_skip_log_cooldown_sec",
        "set:room:economic_min_runner_notional_usdt",
        "set:room:economic_min_fee_mult",
        "set:room:early_invalidation_base_min_hold_sec",
        "set:room:early_invalidation_min_hold_floor_sec",
        "set:room:early_invalidation_max_hold_cap_sec",
        "set:room:early_invalidation_failure_score_threshold",
        "set:room:early_invalidation_patience_score_threshold",
        "set:room:early_invalidation_near_tp_floor_ratio",
        "set:room:early_invalidation_near_tp_grace_sec",
        "set:room:early_invalidation_chop_strictness_mult",
        "set:room:early_invalidation_trend_patience_mult",
        "set:room:symbol_direction_loss_threshold",
        "set:room:symbol_direction_cooldown_minutes",
        "set:room:symbol_direction_loss_conf_bump",
        "set:room:symbol_direction_loss_size_mult",
        "set:room:symbol_direction_memory_decay_hours",
        "set:room:profit_lock_trigger_pct",
        "set:room:profit_lock_buffer_pct",
        "set:room:deployment_target_utilization_pct",
        "set:room:deployment_max_positions",
        "set:room:min_vote_score",
        "set:room:v6_micro_confidence_threshold",
        "set:room:v6_growth_confidence_threshold",
        "set:room:v6_pro_confidence_threshold",
    }:
        key = str(data).split(":")[-1]
        await _render_num_room(key)

    # Enum rooms.
    elif data == "set:room:deployment_mode":
        cur = str(_v("deployment_mode", "Guarded") or "Guarded")
        await edit(
            _tp.panel_room_number(_settings, "deployment_mode", "🚀 Deployment Mode", "Controls capital deployment posture.", fmt="{v}"),
            InlineKeyboardMarkup([
                [
                    InlineKeyboardButton("GUARDED" + (" ●" if cur == "Guarded" else ""), callback_data="room:deploy_mode:Guarded"),
                    InlineKeyboardButton("BAL" + (" ●" if cur == "Balanced" else ""), callback_data="room:deploy_mode:Balanced"),
                ],
                [
                    InlineKeyboardButton("MAX" + (" ●" if cur == "Max Deploy" else ""), callback_data="room:deploy_mode:Max Deploy"),
                    InlineKeyboardButton("FULL" + (" ●" if cur == "Full Deployment" else ""), callback_data="room:deploy_mode:Full Deployment"),
                ],
                [
                    InlineKeyboardButton("SINGLE" + (" ●" if cur == "Single Shot" else ""), callback_data="room:deploy_mode:Single Shot"),
                    InlineKeyboardButton("SWARM" + (" ●" if cur == "Swarm" else ""), callback_data="room:deploy_mode:Swarm"),
                ],
                [
                    InlineKeyboardButton("RECOVERY" + (" ●" if cur == "Recovery" else ""), callback_data="room:deploy_mode:Recovery"),
                ],
                [InlineKeyboardButton("← Feature", callback_data="set:fg:deployment")],
                [InlineKeyboardButton("← Category", callback_data="set:cat:capital")],
                [InlineKeyboardButton("← Settings", callback_data="settings_menu")],
            ]),
        )
    elif data == "set:room:exposure_control_mode":
        cur = str(_v("exposure_control_mode", "Strict") or "Strict")
        await edit(
            _tp.panel_room_number(_settings, "exposure_control_mode", "📉 Exposure Mode", "Controls portfolio exposure gating.", fmt="{v}"),
            InlineKeyboardMarkup([
                [
                    InlineKeyboardButton("STRICT" + (" ●" if cur == "Strict" else ""), callback_data="room:exp_mode:Strict"),
                    InlineKeyboardButton("ADAPT" + (" ●" if cur == "Adaptive" else ""), callback_data="room:exp_mode:Adaptive"),
                ],
                [
                    InlineKeyboardButton("RELAX" + (" ●" if cur == "Relaxed" else ""), callback_data="room:exp_mode:Relaxed"),
                    InlineKeyboardButton("OVERRIDE" + (" ●" if cur == "Override" else ""), callback_data="room:exp_mode:Override"),
                ],
                [InlineKeyboardButton("← Feature", callback_data="set:fg:exposure")],
                [InlineKeyboardButton("← Category", callback_data="set:cat:risk")],
                [InlineKeyboardButton("← Settings", callback_data="settings_menu")],
            ]),
        )
    elif str(data).startswith("room:bool:"):
        parts = str(data).split(":")
        key = parts[2] if len(parts) >= 4 else ""
        action = parts[3] if len(parts) >= 4 else ""
        if key in _BOOL_ROOMS and action in {"on", "off"}:
            _settings.set_val(key, action == "on")
            await _render_bool_room(key)
        else:
            await q.answer("Unknown toggle room", show_alert=True)
    elif str(data).startswith("room:num:"):
        parts = str(data).split(":")
        key = parts[2] if len(parts) >= 4 else ""
        action = parts[3] if len(parts) >= 4 else ""
        meta = _NUM_ROOMS.get(key)
        if meta and action == "custom" and bool(meta.get("custom")):
            chat_mode[uid] = f"await_numeric_setting:{key}"
            await q.answer("Send the new value (zero or greater).", show_alert=True)
        elif meta and action in {"up", "dn"}:
            try:
                current = float(_v(key, 0.0) or 0.0)
            except Exception:
                current = 0.0
            step = float(meta.get("step", 1.0) or 1.0)
            next_val = current + step if action == "up" else current - step
            next_val = max(float(meta.get("min", next_val)), min(float(meta.get("max", next_val)), next_val))
            if bool(meta.get("as_int", False)):
                _settings.set_val(key, int(round(next_val)))
            else:
                _settings.set_val(key, round(next_val, 6))
            await _render_num_room(key)
        else:
            await q.answer("Unknown numeric room", show_alert=True)
    elif str(data).startswith("room:deploy_mode:"):
        next_mode = str(data).split(":", 2)[-1]
        if next_mode == "AI Adaptive":
            await q.answer("AI deployment mode was removed.", show_alert=True)
            await edit(_tp.panel_room_number(_settings, "deployment_mode", "🚀 Deployment Mode", "Controls capital deployment posture.", fmt="{v}"),
                       InlineKeyboardMarkup([[InlineKeyboardButton("← Feature", callback_data="set:fg:deployment")]]))
        else:
            _apply_deployment_mode_choice(next_mode)
            await edit(_tp.panel_room_number(_settings, "deployment_mode", "🚀 Deployment Mode", "Controls capital deployment posture.", fmt="{v}"),
                       InlineKeyboardMarkup([[InlineKeyboardButton("← Feature", callback_data="set:fg:deployment"),
                                              InlineKeyboardButton("← Settings", callback_data="settings_menu")]]))
    elif str(data).startswith("room:exp_mode:"):
        next_mode = str(data).split(":", 2)[-1]
        _apply_exposure_control_mode(next_mode)
        await edit(_tp.panel_room_number(_settings, "exposure_control_mode", "📉 Exposure Mode", "Controls portfolio exposure gating.", fmt="{v}"),
                   InlineKeyboardMarkup([[InlineKeyboardButton("← Feature", callback_data="set:fg:exposure"),
                                          InlineKeyboardButton("← Settings", callback_data="settings_menu")]]))
    elif str(data).startswith("room:global_slot_mode:"):
        mode = str(data).split(":", 2)[-1].strip().lower()
        if mode in {"fixed", "auto"}:
            _settings.set_val("global_slot_mode", mode)
            await edit(_tp.panel_room_global_slot_mode(_settings), _tp.kb_room_global_slot_mode(_settings))
        else:
            await q.answer("Unknown slot mode", show_alert=True)
    elif str(data).startswith("room:max_open_positions:"):
        action = str(data).split(":")[-1]
        cur = int(_v("max_open_positions", 10) or 10)
        if action == "up":
            cur = min(cur + 1, 99)
        elif action == "dn":
            cur = max(cur - 1, 1)
        else:
            await q.answer("Unknown max-position action", show_alert=True)
            return
        _settings.set_val("max_open_positions", cur)
        await edit(_tp.panel_room_max_open_positions(_settings), _tp.kb_room_max_open_positions(_settings))
    elif str(data).startswith("room:trade_selection_policy:"):
        policy = str(data).split(":", 2)[-1].strip().lower()
        if policy in {"first_come", "best_score", "replace_weakest"}:
            _settings.set_val("trade_selection_policy", policy)
            await edit(_tp.panel_room_trade_selection_policy(_settings), _tp.kb_room_trade_selection_policy(_settings))
        else:
            await q.answer("Unknown selection policy", show_alert=True)
    elif str(data).startswith("room:tp_mode:"):
        mode = str(data).split(":", 2)[-1].strip().lower()
        if mode in {"off", "reference_only", "exchange_tp", "hybrid"}:
            _settings.set_val("tp_mode", mode)
            await edit(_tp.panel_room_tp_mode(_settings), _tp.kb_room_tp_mode(_settings))
        else:
            await q.answer("Unknown TP mode", show_alert=True)
    elif str(data).startswith("room:size_mode:"):
        mode = str(data).split(":", 2)[-1].strip().lower()
        if mode in {"notional", "balance", "risk", "blended"}:
            _settings.set_val("size_mode", mode)
            await edit(_tp.panel_room_size_mode(_settings), _tp.kb_room_size_mode(_settings))
        else:
            await q.answer("Unknown size mode", show_alert=True)
    elif str(data).startswith("room:engine_cap_set:"):
        parts = str(data).split(":")
        eng = parts[2].upper() if len(parts) >= 5 else ""
        mode = parts[3].lower() if len(parts) >= 5 else ""
        page = int(parts[4]) if len(parts) >= 5 and str(parts[4]).isdigit() else 0
        if eng and mode in {"inherit", "unlimited"}:
            _settings.set_val(f"engine_{eng.lower()}_slot_mode", mode)
            _settings.set_val(f"engine_{eng.lower()}_max_pos", 0 if mode == "inherit" else -1)
            await edit(_tp.panel_room_engine_cap_text(_settings, eng), _tp.kb_room_engine_cap(_settings, eng, back_page=page))
        else:
            await q.answer("Unknown engine cap mode", show_alert=True)
    elif str(data).startswith("room:engine_cap_adj:"):
        parts = str(data).split(":")
        eng = parts[2].upper() if len(parts) >= 5 else ""
        action = parts[3].lower() if len(parts) >= 5 else ""
        page = int(parts[4]) if len(parts) >= 5 and str(parts[4]).isdigit() else 0
        if not eng or action not in {"up", "dn"}:
            await q.answer("Unknown engine cap action", show_alert=True)
            return
        key_mode = f"engine_{eng.lower()}_slot_mode"
        key_cap = f"engine_{eng.lower()}_max_pos"
        cur = int(_v(key_cap, 1) or 1)
        if cur <= 0:
            cur = 1
        cur = min(cur + 1, 99) if action == "up" else max(cur - 1, 1)
        _settings.set_val(key_mode, "fixed")
        _settings.set_val(key_cap, cur)
        await edit(_tp.panel_room_engine_cap_text(_settings, eng), _tp.kb_room_engine_cap(_settings, eng, back_page=page))
    elif str(data).startswith("room:engine_cap:"):
        parts = str(data).split(":")
        eng = parts[2].upper() if len(parts) >= 3 else ""
        page = int(parts[3]) if len(parts) >= 4 and str(parts[3]).isdigit() else 0
        if eng:
            key_mode = f"engine_{eng.lower()}_slot_mode"
            key_cap = f"engine_{eng.lower()}_max_pos"
            if str(_v(key_mode, "inherit") or "inherit").lower() != "fixed":
                _settings.set_val(key_mode, "fixed")
                _settings.set_val(key_cap, max(int(_v(key_cap, 1) or 1), 1))
            await edit(_tp.panel_room_engine_cap_text(_settings, eng), _tp.kb_room_engine_cap(_settings, eng, back_page=page))
        else:
            await q.answer("Unknown engine", show_alert=True)
    elif data == "stg_lev_up":
        _settings.set_val("leverage_default", min(int(_v("leverage_default",10))+1, int(_v("leverage_max",15))))
        _text, _kb = await _strategy_panel_payload()
        await edit(_text, _kb)
    elif data == "stg_lev_dn":
        _settings.set_val("leverage_default", max(int(_v("leverage_default",10))-1, int(_v("leverage_min",5))))
        _text, _kb = await _strategy_panel_payload()
        await edit(_text, _kb)
    elif data == "stg_votes_up":
        _settings.set_val("min_vote_score", round(min(float(_v("min_vote_score", 3.0)) + 0.1, 3.0), 1))
        _text, _kb = await _strategy_panel_payload()
        await edit(_text, _kb)
    elif data == "stg_votes_dn":
        _settings.set_val("min_vote_score", round(max(float(_v("min_vote_score", 3.0)) - 0.1, 1.0), 1))
        _text, _kb = await _strategy_panel_payload()
        await edit(_text, _kb)
    elif data in ("stg_exec_cycle", "stg_exec_cycle_strategy"):
        modes = ["market", "limit", "staged"]
        cur = str(_v("v6_execution_mode", "market")).lower()
        idx = modes.index(cur) if cur in modes else 0
        _settings.set_val("v6_execution_mode", modes[(idx + 1) % len(modes)])
        if data == "stg_exec_cycle_strategy":
            _text, _kb = await _strategy_panel_payload()
        else:
            _text, _kb = _tp.panel_settings_execution_text(_settings), _tp.kb_settings_execution(_settings)
        await edit(_text, _kb)
    elif data == "stg_risk_up":
        _settings.set_val("risk_pct", round(min(float(_v("risk_pct",2.0))+0.25, float(_v("risk_pct_max",10.0))), 2))
        _text, _kb = await _risk_panel_payload()
        await edit(_text, _kb)
    elif data in {
        "stg_exposure_mode_strict",
        "stg_exposure_mode_adaptive",
        "stg_exposure_mode_relaxed",
        "stg_exposure_mode_override",
    }:
        mode_map = {
            "stg_exposure_mode_strict": "Strict",
            "stg_exposure_mode_adaptive": "Adaptive",
            "stg_exposure_mode_relaxed": "Relaxed",
            "stg_exposure_mode_override": "Override",
        }
        _apply_exposure_control_mode(mode_map[data])
        _text, _kb = await _risk_panel_payload()
        await edit(_text, _kb)
    elif data == "stg_risk_dn":
        _settings.set_val("risk_pct", round(max(float(_v("risk_pct",2.0))-0.25, 0.25), 2))
        _text, _kb = await _risk_panel_payload()
        await edit(_text, _kb)
    elif data == "stg_size_up":
        new_size = min(float(_v("position_size_usdt", 100.0)) + 10, 1000.0)
        _settings.set_val("position_size_usdt", new_size)
        _text, _kb = await _strategy_panel_payload()
        await edit(_text, _kb)
    elif data == "stg_size_dn":
        new_size = max(float(_v("position_size_usdt", 100.0)) - 10, 25.0)
        _settings.set_val("position_size_usdt", new_size)
        _text, _kb = await _strategy_panel_payload()
        await edit(_text, _kb)
    elif data == "stg_dynmin_up":
        current = float(_v("dynamic_target_min_usdt", 50.0))
        upper = float(_v("dynamic_target_max_usdt", max(current, 100.0)))
        _settings.set_val("dynamic_target_min_usdt", round(min(current + 5.0, upper), 2))
        _text, _kb = await _strategy_panel_payload()
        await edit(_text, _kb)
    elif data == "stg_dynmin_dn":
        current = float(_v("dynamic_target_min_usdt", 50.0))
        _settings.set_val("dynamic_target_min_usdt", round(max(current - 5.0, 10.0), 2))
        _text, _kb = await _strategy_panel_payload()
        await edit(_text, _kb)
    elif data == "stg_dynmax_up":
        current = float(_v("dynamic_target_max_usdt", 100.0))
        _settings.set_val("dynamic_target_max_usdt", round(min(current + 10.0, 500.0), 2))
        _text, _kb = await _strategy_panel_payload()
        await edit(_text, _kb)
    elif data == "stg_dynmax_dn":
        current = float(_v("dynamic_target_max_usdt", 100.0))
        floor = float(_v("dynamic_target_min_usdt", 50.0))
        _settings.set_val("dynamic_target_max_usdt", round(max(current - 10.0, floor), 2))
        _text, _kb = await _strategy_panel_payload()
        await edit(_text, _kb)
    elif data == "stg_livecap_up":
        current = float(_v("v6_max_position_pct_of_balance", 0.05))
        _settings.set_val("v6_max_position_pct_of_balance", round(min(current + 0.01, 0.10), 2))
        _text, _kb = await _strategy_panel_payload()
        await edit(_text, _kb)
    elif data == "stg_livecap_dn":
        current = float(_v("v6_max_position_pct_of_balance", 0.05))
        _settings.set_val("v6_max_position_pct_of_balance", round(max(current - 0.01, 0.02), 2))
        _text, _kb = await _strategy_panel_payload()
        await edit(_text, _kb)
    elif data == "stg_dcap_up":
        _settings.set_val("daily_loss_pct", round(min(float(_v("daily_loss_pct", 12.0)) + 1.0, 30.0), 1))
        _text, _kb = await _risk_panel_payload()
        await edit(_text, _kb)
    elif data == "stg_dcap_dn":
        _settings.set_val("daily_loss_pct", round(max(float(_v("daily_loss_pct", 12.0)) - 1.0, 3.0), 1))
        _text, _kb = await _risk_panel_payload()
        await edit(_text, _kb)
    elif data == "stg_wcap_up":
        _settings.set_val("weekly_cap_threshold_pct", round(min(float(_v("weekly_cap_threshold_pct", _v("weekly_loss_pct", 25.0))) + 2.0, 60.0), 1))
        _text, _kb = await _risk_panel_payload()
        await edit(_text, _kb)
    elif data == "stg_wcap_dn":
        _settings.set_val("weekly_cap_threshold_pct", round(max(float(_v("weekly_cap_threshold_pct", _v("weekly_loss_pct", 25.0))) - 2.0, 5.0), 1))
        _text, _kb = await _risk_panel_payload()
        await edit(_text, _kb)
    elif data in ("stg_wkcap_off", "stg_wkcap_soft", "stg_wkcap_strict", "stg_wkcap_adaptive"):
        mode_map = {
            "stg_wkcap_off": "off",
            "stg_wkcap_soft": "soft",
            "stg_wkcap_strict": "strict",
            "stg_wkcap_adaptive": "adaptive",
        }
        _settings.set_val("weekly_cap_mode", mode_map.get(data, "adaptive"))
        _text, _kb = await _risk_panel_payload()
        await edit(_text, _kb)
    elif data == "stg_maxpos_up":
        # Operator requested uncapped max position toggles. Keep only the lower bound elsewhere.
        _settings.set_val("max_open_positions", int(_v("max_open_positions", 10)) + 1)
        _text, _kb = await _risk_panel_payload()
        await edit(_text, _kb)
    elif data == "stg_maxpos_dn":
        _settings.set_val("max_open_positions", max(int(_v("max_open_positions", 10)) - 1, 1))
        _text, _kb = await _risk_panel_payload()
        await edit(_text, _kb)
    elif data == "stg_openrisk_up":
        _settings.set_val("rule_357_max_open_risk_pct", round(min(float(_v("rule_357_max_open_risk_pct", 25.0)) + 1.0, 50.0), 1))
        _text, _kb = await _risk_panel_payload()
        await edit(_text, _kb)
    elif data == "stg_openrisk_dn":
        _settings.set_val("rule_357_max_open_risk_pct", round(max(float(_v("rule_357_max_open_risk_pct", 25.0)) - 1.0, 5.0), 1))
        _text, _kb = await _risk_panel_payload()
        await edit(_text, _kb)
    elif data == "stg_exp_cap_up":
        exposure_key, headroom_key, _mode = await _active_risk_keys()
        new_cap = round(min(float(_v(exposure_key, 80.0)) + 10.0, 200.0), 1)
        _settings.set_val(exposure_key, new_cap)
        if float(_v(headroom_key, new_cap)) > new_cap:
            _settings.set_val(headroom_key, new_cap)
        _text, _kb = await _risk_panel_payload()
        await edit(_text, _kb)
    elif data == "stg_exp_cap_dn":
        exposure_key, headroom_key, _mode = await _active_risk_keys()
        new_cap = round(max(float(_v(exposure_key, 80.0)) - 10.0, 40.0), 1)
        _settings.set_val(exposure_key, new_cap)
        if float(_v(headroom_key, new_cap)) > new_cap:
            _settings.set_val(headroom_key, new_cap)
        _text, _kb = await _risk_panel_payload()
        await edit(_text, _kb)
    elif data == "stg_headroom_up":
        exposure_key, headroom_key, _mode = await _active_risk_keys()
        exposure_cap = float(_v(exposure_key, 80.0))
        new_cap = round(min(float(_v(headroom_key, exposure_cap)) + 10.0, exposure_cap), 1)
        _settings.set_val(headroom_key, new_cap)
        _text, _kb = await _risk_panel_payload()
        await edit(_text, _kb)
    elif data == "stg_headroom_dn":
        exposure_key, headroom_key, _mode = await _active_risk_keys()
        new_cap = round(max(float(_v(headroom_key, _v(exposure_key, 80.0))) - 10.0, 20.0), 1)
        _settings.set_val(headroom_key, new_cap)
        _text, _kb = await _risk_panel_payload()
        await edit(_text, _kb)
    elif data == "stg_pf_cool_up":
        _settings.set_val("preflight_zero_contract_cooldown_secs", min(float(_v("preflight_zero_contract_cooldown_secs", 1800.0)) + 300.0, 7200.0))
        await edit(_tp.panel_settings_execution_text(_settings), _tp.kb_settings_execution(_settings))
    elif data == "stg_pf_cool_dn":
        _settings.set_val("preflight_zero_contract_cooldown_secs", max(float(_v("preflight_zero_contract_cooldown_secs", 1800.0)) - 300.0, 300.0))
        await edit(_tp.panel_settings_execution_text(_settings), _tp.kb_settings_execution(_settings))
    elif data == "stg_pf_trigger_up":
        _settings.set_val("preflight_zero_contract_trigger_count", min(int(_v("preflight_zero_contract_trigger_count", 2)) + 1, 5))
        await edit(_tp.panel_settings_execution_text(_settings), _tp.kb_settings_execution(_settings))
    elif data == "stg_pf_trigger_dn":
        _settings.set_val("preflight_zero_contract_trigger_count", max(int(_v("preflight_zero_contract_trigger_count", 2)) - 1, 1))
        await edit(_tp.panel_settings_execution_text(_settings), _tp.kb_settings_execution(_settings))
    elif data == "stg_risk_reduce_up":
        _settings.set_val("risk_auto_reduce_factor", round(min(float(_v("risk_auto_reduce_factor", 0.50)) + 0.05, 1.00), 2))
        await edit(_tp.panel_settings_execution_text(_settings), _tp.kb_settings_execution(_settings))
    elif data == "stg_risk_reduce_dn":
        _settings.set_val("risk_auto_reduce_factor", round(max(float(_v("risk_auto_reduce_factor", 0.50)) - 0.05, 0.25), 2))
        await edit(_tp.panel_settings_execution_text(_settings), _tp.kb_settings_execution(_settings))
    elif data == "stg_tp_mode_cycle":
        modes = ["conservative", "balanced", "extended"]
        cur = str(_v("tp_distance_mode", "balanced")).lower()
        idx = modes.index(cur) if cur in modes else 1
        _settings.set_val("tp_distance_mode", modes[(idx + 1) % len(modes)])
        await edit(_tp.panel_settings_execution_text(_settings), _tp.kb_settings_execution(_settings))
    elif data == "stg_tp_attach_cycle":
        modes = ["off", "reference_only", "exchange_tp", "hybrid"]
        cur = str(_v("tp_mode", "reference_only") or "reference_only").strip().lower()
        idx = modes.index(cur) if cur in modes else 1
        _settings.set_val("tp_mode", modes[(idx + 1) % len(modes)])
        await edit(_tp.panel_settings_execution_text(_settings), _tp.kb_settings_execution(_settings))
    elif data == "stg_intent_cycle":
        cycle = ["", "AGGRESSIVE", "NORMAL", "DEFENSIVE", "STOPPED"]
        cur = str(_v("trade_intent_mode_override", "") or "").upper()
        idx = cycle.index(cur) if cur in cycle else 0
        _settings.set_val("trade_intent_mode_override", cycle[(idx + 1) % len(cycle)])
        _bal = await _settings_balance()
        await edit(
            _tp.panel_settings_advanced_text(_settings, balance=_bal),
            _tp.kb_settings_advanced(_settings, balance=_bal),
        )
    elif data == "stg_preset_ai":
        await q.answer("Removed preset.", show_alert=True)
        _text, _kb = await _modes_panel_payload()
        await edit(_text, _kb)
    elif data in {
        "stg_preset_safe",
        "stg_preset_balanced",
        "stg_preset_normal",          # legacy label
        "stg_preset_aggressive",
        "stg_preset_scalp",
        "stg_preset_adaptive",
        "stg_preset_full_deployment", # legacy power mode
    }:
        old_preset = str(_v("trading_mode_preset", "") or "custom").strip().lower() or "custom"
        preset_map = {
            "stg_preset_safe": _tp.SAFE_MODE_SETTINGS,
            "stg_preset_balanced": _tp.NORMAL_MODE_SETTINGS,
            "stg_preset_normal": _tp.NORMAL_MODE_SETTINGS,
            "stg_preset_aggressive": _tp.AGGRESSIVE_MODE_SETTINGS,
            "stg_preset_scalp": _tp.SCALP_MODE_SETTINGS,
            "stg_preset_adaptive": _tp.ADAPTIVE_MODE_SETTINGS,
            "stg_preset_full_deployment": _tp.FULL_DEPLOYMENT_MODE_SETTINGS,
        }
        _tp.apply_preset(_settings, preset_map[data])
        new_preset = str(_v("trading_mode_preset", "") or "custom").strip().lower() or "custom"
        if old_preset != new_preset:
            logger.info("MODE APPLY: preset %s -> %s", old_preset, new_preset)
        _text, _kb = await _modes_panel_payload()
        await edit(_text, _kb)
    elif data in {"stg_profile_auto", "stg_profile_micro", "stg_profile_growth", "stg_profile_pro"}:
        profile_map = {
            "stg_profile_auto": "AUTO",
            "stg_profile_micro": "MICRO",
            "stg_profile_growth": "GROWTH",
            "stg_profile_pro": "PRO",
        }
        _settings.set_val("capital_profile", profile_map[data])
        _text, _kb = await _modes_panel_payload()
        await edit(_text, _kb)
    elif data == "stg_cap_profile_cycle":
        cycle = ["AUTO", "MICRO", "GROWTH", "PRO"]
        cur = str(_v("capital_profile", "AUTO") or "AUTO").upper()
        idx = cycle.index(cur) if cur in cycle else 0
        _settings.set_val("capital_profile", cycle[(idx + 1) % len(cycle)])
        _text, _kb = await _modes_panel_payload()
        await edit(_text, _kb)
    elif data == "stg_deploy_ai_adaptive":
        await q.answer("Removed deployment mode.", show_alert=True)
        _text, _kb = await _modes_panel_payload()
        await edit(_text, _kb)
    elif data in {
        "stg_deploy_guarded",
        "stg_deploy_balanced",
        "stg_deploy_max_deploy",
        "stg_deploy_full_deployment",
        "stg_deploy_single_shot",
        "stg_deploy_swarm",
        "stg_deploy_recovery",
    }:
        mode_map = {
            "stg_deploy_guarded": "Guarded",
            "stg_deploy_balanced": "Balanced",
            "stg_deploy_max_deploy": "Max Deploy",
            "stg_deploy_full_deployment": "Full Deployment",
            "stg_deploy_single_shot": "Single Shot",
            "stg_deploy_swarm": "Swarm",
            "stg_deploy_recovery": "Recovery",
        }
        _apply_deployment_mode_choice(mode_map[data])
        _text, _kb = await _modes_panel_payload()
        await edit(_text, _kb)
    elif data == "stg_deploy_mode_cycle":
        cycle = ["Guarded", "Balanced", "Max Deploy", "Full Deployment", "Single Shot", "Swarm", "Recovery"]
        cur = str(_v("deployment_mode", cycle[0]) or cycle[0])
        idx = cycle.index(cur) if cur in cycle else 0
        next_mode = cycle[(idx + 1) % len(cycle)]
        _apply_deployment_mode_choice(next_mode)
        _text, _kb = await _modes_panel_payload()
        await edit(_text, _kb)
    elif data == "stg_deploy_target_up":
        current = float(_v("deployment_target_utilization_pct", 0.0) or 0.0)
        baseline = current
        if baseline <= 0:
            try:
                from capital_manager import get_profile as _cap_profile
                baseline = float(_cap_profile(await _settings_balance()).deployment_target_utilization_pct or 15.0)
            except Exception:
                baseline = 15.0
        _settings.set_val("deployment_target_utilization_pct", round(min(baseline + 5.0, 95.0), 1))
        _text, _kb = await _modes_panel_payload()
        await edit(_text, _kb)
    elif data == "stg_deploy_target_dn":
        current = float(_v("deployment_target_utilization_pct", 0.0) or 0.0)
        baseline = current
        if baseline <= 0:
            try:
                from capital_manager import get_profile as _cap_profile
                baseline = float(_cap_profile(await _settings_balance()).deployment_target_utilization_pct or 15.0)
            except Exception:
                baseline = 15.0
        _settings.set_val("deployment_target_utilization_pct", round(max(baseline - 5.0, 5.0), 1))
        _text, _kb = await _modes_panel_payload()
        await edit(_text, _kb)
    elif data == "stg_deploy_maxpos_up":
        current = int(_v("deployment_max_positions", 0) or 0)
        baseline = current
        if baseline <= 0:
            try:
                from capital_manager import get_profile as _cap_profile
                baseline = int(_cap_profile(await _settings_balance()).deployment_max_positions or 2)
            except Exception:
                baseline = 2
        # Operator requested uncapped max position toggles.
        _settings.set_val("deployment_max_positions", baseline + 1)
        _text, _kb = await _modes_panel_payload()
        await edit(_text, _kb)
    elif data == "stg_deploy_maxpos_dn":
        current = int(_v("deployment_max_positions", 0) or 0)
        baseline = current
        if baseline <= 0:
            try:
                from capital_manager import get_profile as _cap_profile
                baseline = int(_cap_profile(await _settings_balance()).deployment_max_positions or 2)
            except Exception:
                baseline = 2
        _settings.set_val("deployment_max_positions", max(baseline - 1, 1))
        _text, _kb = await _modes_panel_payload()
        await edit(_text, _kb)
    elif data == "stg_ptp1_up":
        _settings.set_val("partial_tp1_close_pct", _bounded_partial_pct("partial_tp1_close_pct", "partial_tp2_close_pct", 5.0))
        await edit(_tp.panel_settings_execution_text(_settings), _tp.kb_settings_execution(_settings))
    elif data == "stg_ptp1_dn":
        _settings.set_val("partial_tp1_close_pct", _bounded_partial_pct("partial_tp1_close_pct", "partial_tp2_close_pct", -5.0))
        await edit(_tp.panel_settings_execution_text(_settings), _tp.kb_settings_execution(_settings))
    elif data == "stg_ptp2_up":
        _settings.set_val("partial_tp2_close_pct", _bounded_partial_pct("partial_tp2_close_pct", "partial_tp1_close_pct", 5.0))
        await edit(_tp.panel_settings_execution_text(_settings), _tp.kb_settings_execution(_settings))
    elif data == "stg_ptp2_dn":
        _settings.set_val("partial_tp2_close_pct", _bounded_partial_pct("partial_tp2_close_pct", "partial_tp1_close_pct", -5.0))
        await edit(_tp.panel_settings_execution_text(_settings), _tp.kb_settings_execution(_settings))
    elif data == "stg_uspread_up":
        _settings.set_val("v6_spread_max_pct", round(min(float(_v("v6_spread_max_pct", 0.30)) + 0.05, 5.0), 2))
        await edit(_tp.panel_settings_filters_text(_settings), _tp.kb_settings_filters(_settings))
    elif data == "stg_uspread_dn":
        _settings.set_val("v6_spread_max_pct", round(max(float(_v("v6_spread_max_pct", 0.30)) - 0.05, 0.05), 2))
        await edit(_tp.panel_settings_filters_text(_settings), _tp.kb_settings_filters(_settings))
    elif data == "stg_uvol_up":
        _settings.set_val("v6_min_volume_usd_24h", _stepped_min_volume(float(_v("v6_min_volume_usd_24h", 2_000_000.0)), 1))
        await edit(_tp.panel_settings_filters_text(_settings), _tp.kb_settings_filters(_settings))
    elif data == "stg_uvol_dn":
        _settings.set_val("v6_min_volume_usd_24h", _stepped_min_volume(float(_v("v6_min_volume_usd_24h", 2_000_000.0)), -1))
        await edit(_tp.panel_settings_filters_text(_settings), _tp.kb_settings_filters(_settings))
    elif data in ("stg_leverage_info", "stg_votes_info", "stg_size_info", "stg_livecap_info", "stg_dynmin_info", "stg_dynmax_info"):
        _text, _kb = await _strategy_panel_payload()
        await edit(_text, _kb)
    elif data in ("stg_wr_off", "stg_wr_bal", "stg_wr_strict", "stg_wr_adapt"):
        if data == "stg_wr_off":
            _settings.set_val("winrate_optimization_enabled", False)
        else:
            _settings.set_val("winrate_optimization_enabled", True)
            prof_map = {
                "stg_wr_bal": "balanced",
                "stg_wr_strict": "strict",
                "stg_wr_adapt": "adaptive",
            }
            _settings.set_val("winrate_profile", prof_map.get(data, "balanced"))
        _text, _kb = await _strategy_panel_payload()
        await edit(_text, _kb)
    elif data in ("stg_deploy_target_info", "stg_deploy_maxpos_info"):
        _text, _kb = await _modes_panel_payload()
        await edit(_text, _kb)
    elif data in ("stg_risk_info", "stg_dcap_info", "stg_wcap_info", "stg_maxpos_info", "stg_openrisk_info", "stg_exp_cap_info", "stg_headroom_info"):
        _text, _kb = await _risk_panel_payload()
        await edit(_text, _kb)
    elif data in ("stg_pf_cool_info", "stg_pf_trigger_info", "stg_ptp1_info", "stg_ptp2_info", "stg_risk_reduce_info"):
        await edit(_tp.panel_settings_execution_text(_settings), _tp.kb_settings_execution(_settings))
    elif data in ("stg_uspread_info", "stg_uvol_info"):
        await edit(_tp.panel_settings_filters_text(_settings), _tp.kb_settings_filters(_settings))
    elif data in ("stg_global_slot_fixed", "stg_global_slot_auto"):
        mode_map = {
            "stg_global_slot_fixed": "fixed",
            "stg_global_slot_auto": "auto",
        }
        _settings.set_val("global_slot_mode", mode_map.get(data, "fixed"))
        _bal = await _settings_balance()
        await edit(_tp.panel_settings_slots_text(_settings, balance=_bal), _tp.kb_settings_slots(_settings, balance=_bal))
    elif data in ("stg_sel_first", "stg_sel_best", "stg_sel_replace"):
        sel_map = {
            "stg_sel_first": "first_come",
            "stg_sel_best": "best_score",
            "stg_sel_replace": "replace_weakest",
        }
        _settings.set_val("trade_selection_policy", sel_map.get(data, "replace_weakest"))
        _bal = await _settings_balance()
        await edit(_tp.panel_settings_slots_text(_settings, balance=_bal), _tp.kb_settings_slots(_settings, balance=_bal))
    elif data == "stg_slots_up":
        cur = int(_v("max_open_positions", 10) or 10)
        # Operator requested uncapped max position toggles.
        _settings.set_val("max_open_positions", cur + 1)
        _bal = await _settings_balance()
        await edit(_tp.panel_settings_slots_text(_settings, balance=_bal), _tp.kb_settings_slots(_settings, balance=_bal))
    elif data == "stg_slots_dn":
        cur = int(_v("max_open_positions", 10) or 10)
        _settings.set_val("max_open_positions", max(cur - 1, 1))
        _bal = await _settings_balance()
        await edit(_tp.panel_settings_slots_text(_settings, balance=_bal), _tp.kb_settings_slots(_settings, balance=_bal))
    elif data == "stg_slots_info":
        await q.answer("Fixed mode uses Max Pos. Auto/Unlimited compute slots dynamically. Changes affect NEW entries.", show_alert=False)
    elif data.startswith("stg_engslot:"):
        parts = str(data).split(":")
        eng = parts[1].upper() if len(parts) >= 2 else ""
        action = parts[2].lower() if len(parts) >= 3 else ""
        try:
            back_page = int(parts[3]) if len(parts) >= 4 else 0
        except Exception:
            back_page = 0
        if eng:
            if action == "inherit":
                _settings.set_val(f"engine_{eng.lower()}_slot_mode", "inherit")
                _settings.set_val(f"engine_{eng.lower()}_max_pos", 0)
            elif action == "unlimited":
                _settings.set_val(f"engine_{eng.lower()}_slot_mode", "unlimited")
                _settings.set_val(f"engine_{eng.lower()}_max_pos", -1)
            elif action in {"up", "dn"}:
                raw = _settings.get_val(f"engine_{eng.lower()}_max_pos", 1)
                try:
                    cur = int(raw) if raw is not None else 1
                except Exception:
                    cur = 1
                cur = cur if cur > 0 else 1
                if action == "up":
                    cur += 1
                else:
                    cur = max(cur - 1, 1)
                _settings.set_val(f"engine_{eng.lower()}_slot_mode", "fixed")
                _settings.set_val(f"engine_{eng.lower()}_max_pos", cur)
            else:
                try:
                    n = int(action)
                except Exception:
                    n = 0
                if n > 0:
                    _settings.set_val(f"engine_{eng.lower()}_slot_mode", "fixed")
                    _settings.set_val(f"engine_{eng.lower()}_max_pos", n)
        _bal = await _settings_balance()
        await edit(
            _tp.panel_settings_engine_slot_detail_text(_settings, eng, balance=_bal),
            _tp.kb_settings_engine_slot_detail(_settings, eng, back_page=back_page),
        )
    elif data == "stg_engines":
        await edit(_tp.panel_settings_engines_text(_settings), engines_settings_kb())
    elif data.startswith("stg_strategy_tog_"):
        # Backward-compatible handler for older strategy panels.
        key = data.replace("stg_strategy_tog_", "")
        _settings.set_val(key, not _v(key))
        _text, _kb = await _strategy_panel_payload()
        await edit(_text, _kb)
    elif data.startswith("stg_eng_tog_"):
        name = data.replace("stg_eng_tog_","").upper()
        _mode_cycle = ["disabled", "shadow", "probation", "live"]
        cur = _settings.get_engine_mode(name)
        idx = _mode_cycle.index(cur) if cur in _mode_cycle else 0
        new_mode = _mode_cycle[(idx + 1) % len(_mode_cycle)]
        _settings.set_engine_mode(name, new_mode, actor="telegram_settings", reason="settings toggle")
        await q.answer(f"{name} → {new_mode.upper()}")
        await edit(_tp.panel_settings_engines_text(_settings), engines_settings_kb())
    elif data == "stg_reset_confirm":
        await edit(
            "⚠️ *Reset to defaults?*\n\nThis will restore all sizing, leverage, and caps to their "
            "default values. Engine modes will NOT be changed.\n\n_This cannot be undone._",
            InlineKeyboardMarkup([
                [InlineKeyboardButton("✓ Yes, reset", callback_data="stg_reset_confirmed"),
                 InlineKeyboardButton("✗ Cancel",     callback_data="settings_menu")],
            ])
        )
    elif data == "stg_reset_confirmed":
        _settings.reset_to_defaults()
        _text, _kb = await _settings_dashboard_payload()
        await edit(_text, _kb)
    elif data.startswith("stg_tog_"):
        key = data.replace("stg_tog_","")
        _settings.set_val(key, not _v(key))
        _PARTICIPATION_KEYS = {
            "lane_budget_enabled","lane_override_enabled",
            "soft_accept_enabled","entry_filter_enabled","htf_filter_enabled",
            "session_filter_enabled","correlation_filter_enabled",
            "same_direction_headroom_enabled","per_engine_cap_enabled",
            "engine_weight_adaptation_enabled","confirm_path_enabled",
            "safe_reentry_enabled",
        }
        _EXECUTION_KEYS = {
            "dynamic_sizing_enabled","v6_dynamic_sizing_enabled",
            "leverage_fallback_enabled","preflight_cooldown_enabled",
            "risk_floor_preserve_enabled","pre_reserve_size_validation_enabled",
            "v6_execution_allow_limit","v6_execution_allow_staged",
            "secure_profit_mode_enabled","breakeven_enabled","tsl_enabled","partial_exit_enabled",
        }
        _FILTER_KEYS = {
            "spread_filter_enabled","dead_coin_suppression_enabled",
            "executability_penalty_enabled","market_cap_bonus_enabled",
            "small_account_large_cap_preference_enabled",
        }
        _ADVANCED_KEYS = {
            "v6_enabled","v6_weighted_confidence_enabled","v6_capital_adaptive_enabled",
            "v6_smart_execution_enabled","v6_market_filter_enabled","v6_trade_frequency_enabled",
        }
        _DIAGNOSTIC_KEYS = {
            "trade_intent_enabled","soft_kill_switch_enabled",
            "engine_consensus_enabled","block_diagnostics_enabled","headroom_scaling_enabled",
            "slot_rotation_enabled",
        }
        if key in _PARTICIPATION_KEYS:
            await edit(_tp.panel_settings_participation_text(_settings), _tp.kb_settings_participation(_settings))
        elif key in _EXECUTION_KEYS:
            await edit(_tp.panel_settings_execution_text(_settings), _tp.kb_settings_execution(_settings))
        elif key in _FILTER_KEYS:
            await edit(_tp.panel_settings_filters_text(_settings), _tp.kb_settings_filters(_settings))
        elif key in _DIAGNOSTIC_KEYS:
            _bal = await _settings_balance()
            await edit(_tp.panel_settings_advanced_text(_settings, balance=_bal), _tp.kb_settings_advanced(_settings, balance=_bal))
        elif key in _ADVANCED_KEYS:
            _bal = await _settings_balance()
            await edit(_tp.panel_settings_advanced_text(_settings, balance=_bal), _tp.kb_settings_advanced(_settings, balance=_bal))
        else:
            _text, _kb = await _settings_dashboard_payload()
            await edit(_text, _kb)

    # ── V6.1 Panel callbacks ──────────────────────────────────────────────────

    elif data == "p:trades":
        from performance_tracker import _state as _ps
        trades = list(_ps.trades)[-20:] if _ps.trades else []
        open_count = len(engine.all_open_positions())
        await edit(_tp.panel_trades_text(trades=trades, open_count=open_count), _tp.kb_trades())

    elif data == "p:intel":
        try:
            from engine_weights import get_all_weights as _ew
            weights = _ew()
        except Exception: weights = {}
        try: regime = _regime.get_regime()
        except Exception: regime = "—"
        try: session = killzone_status()
        except Exception: session = "—"
        import rejection_log as _rjlog
        rej_summary = _rjlog.get_summary()
        await edit(_tp.panel_intel_text(
            weights=weights, regime=regime, session=session,
            rejection_summary=rej_summary if rej_summary else None,
        ), _tp.kb_intel())

    elif data == "intel:weights":
        try:
            from engine_weights import get_all_weights as _ew
            weights = _ew()
        except Exception: weights = {}
        await edit(_tp.panel_weights_text(weights=weights),
                   InlineKeyboardMarkup([[InlineKeyboardButton("← Intelligence", callback_data="p:intel")]]))

    elif data == "intel:blocked":
        import rejection_log as _rjlog
        rejections = _rjlog.get_recent(25)
        await edit(
            _tp.panel_blocked_text(rejections),
            InlineKeyboardMarkup([
                [InlineKeyboardButton("🔄 Refresh",       callback_data="intel:blocked"),
                 InlineKeyboardButton("🧹 Clear",         callback_data="intel:blocked_clear")],
                [InlineKeyboardButton("← Intelligence",   callback_data="p:intel")],
            ]),
        )

    elif data == "intel:blocked_clear":
        import rejection_log as _rjlog
        _rjlog.clear()
        await edit("🧹 Rejection log cleared.",
                   InlineKeyboardMarkup([[InlineKeyboardButton("← Intelligence", callback_data="p:intel")]]))

    elif data == "p:risk":
        try: bal = await okx.get_usdt_balance()
        except: bal = 0.0
        capital_profile = dd_state = exposure = lane_statuses = None
        try:
            if _v("v6_capital_adaptive_enabled", True):
                from capital_manager import get_profile as _cm
                capital_profile = _cm(bal)
        except Exception: pass
        try:
            if _v("v6_anti_drawdown_enabled", True):
                from drawdown_guard import get_drawdown_state as _dds
                dd_state = _dds(capital_profile=capital_profile)
        except Exception: pass
        try:
            if _v("v6_correlation_control_enabled", True):
                from exposure_tracker import get_exposure as _gexp
                exposure = _gexp(bal)
        except Exception: pass
        try:
            from portfolio_allocator import summarize_lane_usage as _lane_usage
            lane_statuses = _lane_usage(bal, capital_profile=capital_profile)
        except Exception: pass
        await edit(_tp.panel_risk_text(
            bal=bal, capital_profile=capital_profile,
            dd_state=dd_state, exposure=exposure,
            lane_statuses=lane_statuses,
            settings=_settings,
        ), _tp.kb_risk())

    elif data == "risk:drawdown":
        try: bal = await okx.get_usdt_balance()
        except: bal = 0.0
        capital_profile = None
        try:
            from capital_manager import get_profile as _cm
            capital_profile = _cm(bal)
        except Exception: pass
        try:
            from drawdown_guard import get_drawdown_state as _dds
            dd_state = _dds(capital_profile=capital_profile)
            await edit(_tp.panel_drawdown_text(dd_state=dd_state, capital_profile=capital_profile),
                       InlineKeyboardMarkup([[InlineKeyboardButton("← Risk", callback_data="p:risk")]]))
        except Exception as e:
            await edit(f"⚠️ Drawdown data unavailable: `{e}`",
                       InlineKeyboardMarkup([[InlineKeyboardButton("← Risk", callback_data="p:risk")]]))

    elif data == "risk:exposure":
        try: bal = await okx.get_usdt_balance()
        except: bal = 0.0
        try:
            from exposure_tracker import get_exposure as _gexp
            exposure = _gexp(bal)
            await edit(_tp.panel_exposure_text(exposure=exposure, bal=bal),
                       InlineKeyboardMarkup([[InlineKeyboardButton("← Risk", callback_data="p:risk")]]))
        except Exception as e:
            await edit(f"⚠️ Exposure data unavailable: `{e}`",
                       InlineKeyboardMarkup([[InlineKeyboardButton("← Risk", callback_data="p:risk")]]))

    elif data == "p:control":
        running = engine.running
        capital_profile = None
        try:
            if _v("v6_capital_adaptive_enabled", True):
                bal = await okx.get_usdt_balance()
                from capital_manager import get_profile as _cm
                capital_profile = _cm(bal)
        except Exception: pass
        await edit(_tp.panel_control_text(
            running=running,
            running_count=engine.running_count(),
            enabled_count=_enabled_engine_count(),
            capital_profile=capital_profile,
            v6_on=bool(_v("v6_enabled", False)),
            ai_gate=bool(_v("v6_ai_veto_enabled", False)),
            engine_adapt=bool(_v("engine_weight_adaptation_enabled", False)),
            exec_mode=str(_v("v6_execution_mode", "market")),
            preset=str(_v("trading_mode_preset", "") or ""),
        ), _tp.kb_control(running))

    elif data == "c:pause_confirm":
        await edit(
            "🚨 *DISARM new entries?*\n\nScanning continues. Open positions, protection, exits, and reconciliation remain managed.",
            _tp.kb_confirm("c:pause_ok", "p:control"),
        )

    elif data == "c:pause_ok":
        from telegram_operator_controls import disarm as _operator_disarm
        result = await _operator_disarm(engine)
        snap = result["state"]
        await edit(
            f"{_live_state_badge(snap)}\n\nNew entries are blocked. Scanning, exits, protection, and reconciliation remain running.",
            _tp.kb_control(engine.running),
        )

    elif data == "c:resume":
        from telegram_operator_controls import arm as _operator_arm
        result = await _operator_arm(okx)
        snap = result["state"]
        await edit(
            f"{_live_state_badge(snap)}\n\nPreflight: `{'PASS' if result['preflight'].get('ok') else 'BLOCKED'}`\nReason: `{result.get('reason', '')}`",
            _tp.kb_control(engine.running),
        )

    elif data == "c:safe_confirm":
        await edit(_tp.SAFE_MODE_TEXT, _tp.kb_confirm("c:safe_ok", "p:control"))

    elif data == "c:safe_ok":
        _tp.apply_preset(_settings, _tp.SAFE_MODE_SETTINGS)
        await edit("🛡 *Safe Mode applied.* Settings updated — reduced risk, tighter filters.",
                   _tp.kb_control(engine.running))

    elif data == "c:aggro_confirm":
        await edit(_tp.AGGRESSIVE_MODE_TEXT, _tp.kb_confirm("c:aggro_ok", "p:control"))

    elif data == "c:aggro_ok":
        _tp.apply_preset(_settings, _tp.AGGRESSIVE_MODE_SETTINGS)
        await edit("⚡ *Aggressive Mode applied.* Settings updated — wider sizing, looser filters.",
                   _tp.kb_control(engine.running))

    elif data == "c:normal_confirm":
        await edit(
            "↩️ *Reset to Normal Mode?*\n\nRestores balanced risk/reward defaults.",
            _tp.kb_confirm("c:normal_ok", "p:control"),
        )

    elif data == "c:normal_ok":
        _tp.apply_preset(_settings, _tp.NORMAL_MODE_SETTINGS)
        await edit("↩️ *Normal Mode restored.*", _tp.kb_control(engine.running))

    elif data == "c:ai_confirm":
        await edit(_tp.panel_control_text(
            running=engine.running,
            running_count=engine.running_count(),
            enabled_count=_enabled_engine_count(),
            capital_profile=None,
            v6_on=bool(_v("v6_enabled", False)),
            ai_gate=False,
            engine_adapt=bool(_v("engine_weight_adaptation_enabled", False)),
            exec_mode=str(_v("v6_execution_mode", "market")),
            preset=str(_v("trading_mode_preset", "") or ""),
        ), _tp.kb_control(engine.running))

    elif data == "c:closeall_confirm":
        await edit(
            "🔴 *Close ALL open positions?*\n\nThis will send market-close orders for every open trade.",
            _tp.kb_confirm("close_all_confirm", "p:control"),
        )

    # ── Settings subpanel callbacks ────────────────────────────────────────────

    elif data == "s:v6core":
        await edit(_tp.panel_settings_v6core_text(_settings), _tp.kb_settings_v6core(_settings))

    elif data == "s:dashboard":
        _text, _kb = await _settings_dashboard_payload()
        await edit(_text, _kb)

    elif data == "s:strategy":
        _text, _kb = await _strategy_panel_payload()
        await edit(_text, _kb)

    elif data in {"s:modes", "s:capital"}:
        _text, _kb = await _modes_panel_payload()
        await edit(_text, _kb)

    elif data == "live:status":
        from live_state import state as _live_state
        snap = _live_state.snapshot()
        await edit(
            "Live State\n\n"
            f"State: `{snap.get('state', 'UNKNOWN')}`\n"
            f"Armed: `{'YES' if snap.get('armed') else 'NO'}`\n"
            f"Reason: `{snap.get('reason', '')}`",
            InlineKeyboardMarkup([[InlineKeyboardButton("Back to Modes", callback_data="s:modes")]]),
        )

    elif data == "live:preflight":
        from live_preflight import run_preflight as _run_live_preflight
        from live_state import state as _live_state
        result = await _run_live_preflight(
            okx,
            refresh_metadata=True,
            persist_state=False,
            invocation_source="telegram_live_preflight",
        )
        snap = _live_state.snapshot()
        await edit(
            _preflight_operator_presentation(result, snap, read_only=True),
            InlineKeyboardMarkup([[InlineKeyboardButton("Back to Modes", callback_data="s:modes")]]),
        )

    elif data == "queue:view":
        from opportunity_queue import queue as _opportunity_queue
        snap = _opportunity_queue.snapshot(limit=8)
        lines = [
            "Opportunity Queue",
            "",
            f"Count: `{snap.get('count', 0)}`",
            f"Margin demand: `${float(snap.get('queued_margin_demand', 0.0) or 0.0):.2f}`",
        ]
        for row in snap.get("top", []):
            expires = max(float(row.get("expiry_timestamp", 0.0) or 0.0) - time.time(), 0.0)
            lines.append(
                f"`{row.get('pair')}` {str(row.get('direction', '')).upper()} · "
                f"`{row.get('engine')}` · score `{float(row.get('current_score', 0.0) or 0.0):.2f}` · "
                f"expires `{expires:.0f}s` · `{row.get('queued_reason', '')}`"
            )
        await edit("\n".join(lines), InlineKeyboardMarkup([[InlineKeyboardButton("Back to Modes", callback_data="s:modes")]]))

    elif data == "live:arm_confirm":
        await edit(
            "Arm Live Trading?\n\nThis runs the production read-only preflight. Arming still fails closed unless PAPER_TRADING=false and every required service passes.",
            InlineKeyboardMarkup([
                [InlineKeyboardButton("Confirm Arm", callback_data="live:arm")],
                [InlineKeyboardButton("Cancel", callback_data="s:modes")],
            ]),
        )

    elif data == "live:arm":
        from telegram_operator_controls import arm as _operator_arm
        result = await _operator_arm(okx, engine)
        snap = result["state"]
        await edit(
            f"Live arm result: `{'ARMED' if result['ok'] else 'BLOCKED'}`\n"
            f"Effective state: `{snap.get('state', 'UNKNOWN')}`\n"
            f"Reason: `{result.get('reason', '')}`",
            InlineKeyboardMarkup([[InlineKeyboardButton("Back to Modes", callback_data="s:modes")]]),
        )

    elif data == "live:disarm":
        from telegram_operator_controls import disarm as _operator_disarm
        result = await _operator_disarm(engine)
        snap = result["state"]
        await edit(
            f"Live state: `{snap.get('state')}`\nArmed: `NO`\n"
            f"Engines running: `{'YES' if result.get('engine_running') else 'NO'}`\n"
            "Exit management: `ACTIVE`",
            InlineKeyboardMarkup([[InlineKeyboardButton("Back to Modes", callback_data="s:modes")]]),
        )

    elif data == "live:halt_confirm":
        await edit(
            "Halt new admissions? Exit management remains available.",
            InlineKeyboardMarkup([
                [InlineKeyboardButton("Confirm Halt", callback_data="live:halt")],
                [InlineKeyboardButton("Cancel", callback_data="s:modes")],
            ]),
        )

    elif data == "live:halt":
        from telegram_operator_controls import halt as _operator_halt
        result = await _operator_halt(engine)
        snap = result["state"]
        await edit(
            f"{_live_state_badge(snap)}\n\nAdmissions halted. Scanning, exits, protection, and reconciliation remain running.",
            InlineKeyboardMarkup([[InlineKeyboardButton("Back to Modes", callback_data="s:modes")]]),
        )

    elif data == "live:emergency_confirm":
        await edit(
            "Emergency close all exchange-tracked positions? This is a real close command in LIVE mode.",
            InlineKeyboardMarkup([
                [InlineKeyboardButton("Confirm Emergency Close", callback_data="live:emergency")],
                [InlineKeyboardButton("Cancel", callback_data="s:modes")],
            ]),
        )

    elif data == "live:emergency":
        from telegram_operator_controls import emergency_close as _operator_emergency_close
        result = await _operator_emergency_close(engine)
        await edit(
            f"{_live_state_badge(result['state'])}\n\n{result.get('close_result', '')}",
            InlineKeyboardMarkup([[InlineKeyboardButton("Back to Modes", callback_data="s:modes")]]),
        )

    elif data == "s:risk_cfg":
        _text, _kb = await _risk_panel_payload()
        await edit(_text, _kb)

    elif data == "s:participation":
        await edit(_tp.panel_settings_participation_text(_settings), _tp.kb_settings_participation(_settings))

    elif data == "s:execution":
        await edit(_tp.panel_settings_execution_text(_settings), _tp.kb_settings_execution(_settings))

    elif data == "s:filters":
        await edit(_tp.panel_settings_filters_text(_settings), _tp.kb_settings_filters(_settings))

    elif data == "s:slots":
        _bal = await _settings_balance()
        await edit(
            _tp.panel_settings_slots_text(_settings, balance=_bal),
            _tp.kb_settings_slots(_settings, balance=_bal),
        )

    elif data.startswith("s:slots_engines:"):
        _bal = await _settings_balance()
        try:
            page = int(str(data).split(":")[-1])
        except Exception:
            page = 0
        await edit(
            _tp.panel_settings_engine_slots_text(_settings, balance=_bal, page=page),
            _tp.kb_settings_engine_slots(_settings, page=page),
        )

    elif data.startswith("s:slots_engine:"):
        _bal = await _settings_balance()
        parts = str(data).split(":")
        eng = parts[2].upper() if len(parts) >= 3 else "?"
        try:
            back_page = int(parts[3]) if len(parts) >= 4 else 0
        except Exception:
            back_page = 0
        await edit(
            _tp.panel_settings_engine_slot_detail_text(_settings, eng, balance=_bal),
            _tp.kb_settings_engine_slot_detail(_settings, eng, back_page=back_page),
        )

    elif data == "s:engines":
        await edit(_tp.panel_settings_engines_text(_settings), engines_settings_kb())

    elif data == "s:diagnostics":
        _bal = await _settings_balance()
        await edit(_tp.panel_settings_diagnostics_text(_settings, balance=_bal), _tp.kb_settings_diagnostics(_settings, balance=_bal))

    elif data == "s:advanced":
        _bal = await _settings_balance()
        await edit(_tp.panel_settings_advanced_text(_settings, balance=_bal), _tp.kb_settings_advanced(_settings, balance=_bal))

    elif data == "s:system":
        await edit(_tp.panel_settings_system_text(_settings), _tp.kb_settings_system(_settings))

    elif data == "s:system_reset":
        await edit(_tp.panel_settings_system_reset_text(_settings), _tp.kb_settings_system_reset(_settings))

    elif data == "sys:reset_start":
        await edit(_tp.panel_settings_system_reset_confirm_text(), _tp.kb_system_reset_confirm())

    elif data == "sys:reset_cancel":
        await edit(_tp.panel_settings_system_reset_text(_settings), _tp.kb_settings_system_reset(_settings))

    elif data == "sys:reset_confirm":
        try:
            import reset_manager as _rm
            _rm.reset_all()
            await edit(
                "✅ *New session started*\n"
                "All counters and risk states reset.\n"
                "Live trades unaffected.",
                _tp.kb_system_reset_done(),
            )
        except Exception as e:
            await edit(
                f"⚠️ Reset failed: `{type(e).__name__}`\n\n"
                "_Open trades were not affected._",
                _tp.kb_settings_system_reset(_settings),
            )

    elif data == "diag:blockers":
        import rejection_log as _rjlog
        rejections = _rjlog.get_recent(25)
        await edit(
            _tp.panel_blocked_text(rejections),
            InlineKeyboardMarkup([[InlineKeyboardButton("← Diagnostics", callback_data="s:diagnostics")]]),
        )

    elif data == "diag:slots":
        _bal = await _settings_balance()
        try:
            import position_registry as _pr
            reg = dict(getattr(_pr, "_registry", {}) or {})
        except Exception:
            reg = {}
        import rejection_log as _rjlog
        lines = ["🧷 *Slot Usage*", "━━━━━━━━━━━━━━━━━━", ""]

        slot_mode = str(_v("global_slot_mode", "fixed") or "fixed").lower()
        selection = str(_v("trade_selection_policy", "replace_weakest") or "replace_weakest").lower()
        fixed_max = int(_v("max_open_positions", 10) or 10)

        prof = None
        effective_cap = fixed_max
        slot_meta = {}
        try:
            if _bal > 0 and bool(_v("v6_enabled", False)):
                from capital_manager import get_profile as _get_profile
                prof = _get_profile(float(_bal))
        except Exception:
            prof = None
        try:
            import category_engine as _ce
            effective_cap, slot_meta = _ce.get_effective_global_slot_limit(float(_bal or 0.0), capital_profile=prof)
            effective_cap = int(effective_cap or fixed_max)
        except Exception:
            effective_cap = fixed_max
            slot_meta = {}

        lines.append(f"Global: mode `{slot_mode}` · fixed `{fixed_max}` · effective `{len(reg)}/{effective_cap}`")
        if slot_meta:
            lines.append(f"Dynamic cap: `{slot_meta.get('global_max','—')}` · deploy `{slot_meta.get('deployment_max','—')}` · policy `{slot_meta.get('policy_limit','—')}`")
        lines.append(f"Selection: `{selection}`")

        wr_on = bool(_v("winrate_optimization_enabled", False))
        wr_prof = str(_v("winrate_profile", "balanced") or "balanced").strip().lower()
        if wr_prof not in {"balanced", "strict", "adaptive"}:
            wr_prof = "balanced"
        lines.append(f"Winrate Opt: `{'OFF' if not wr_on else wr_prof.upper()}`")

        try:
            dbg = _settings.get_debug_override_snapshot() or {}
            dbg_mode = str(dbg.get("mode", "off") or "off").upper()
            dbg_active = dbg.get("active", {}) or {}
            if dbg_mode != "OFF" or dbg_active:
                if dbg_active:
                    short = ", ".join(f"{k}={v}" for k, v in sorted(dbg_active.items()))
                    lines.append(f"Debug Overrides: `{dbg_mode}` ({short})")
                else:
                    lines.append(f"Debug Overrides: `{dbg_mode}`")
            else:
                lines.append("Debug Overrides: `OFF`")
        except Exception:
            lines.append("Debug Overrides: `UNKNOWN`")
        try:
            wk_on = bool(_v("weekly_cap_enabled", False))
            wk_hit, wk_pct = _perf.is_weekly_cap_hit(float(_bal or 0.0))
            if wk_on:
                lines.append(f"Weekly Cap: `{'HIT' if wk_hit else 'OK'}` (`{wk_pct:.1f}%`)")
            else:
                lines.append("Weekly Cap: `OFF`")
        except Exception:
            lines.append("Weekly Cap: `UNKNOWN`")

        by_engine = {}
        for v in reg.values():
            eng = str(v.get("engine", "") or "").upper() or "?"
            by_engine[eng] = by_engine.get(eng, 0) + 1
        if by_engine:
            lines.append("")
            lines.append("*Per-engine:*")
            for eng, n in sorted(by_engine.items(), key=lambda kv: (-kv[1], kv[0])):
                eff = "—"
                try:
                    import category_engine as _ce
                    eff = str(int(_ce.get_effective_engine_cap(eng, balance=float(_bal or 0.0), capital_profile=prof) or 0))
                except Exception:
                    eff = "—"
                lines.append(f"• `{eng}` `{n}/{eff}`")

        # Replacement/blocks summary for operator awareness.
        try:
            recent = list(_rjlog.get_recent(60) or [])
            counts = {"global_cap": 0, "engine_cap": 0, "replaced_by_better_trade": 0}
            for r in recent:
                tag = str(getattr(r, "tag", "") or "")
                if tag in counts:
                    counts[tag] += 1
            lines.append("")
            lines.append(f"Recent (60): global_cap `{counts['global_cap']}` · engine_cap `{counts['engine_cap']}` · replaced `{counts['replaced_by_better_trade']}`")
        except Exception:
            pass
        await edit(
            "\n".join(lines),
            InlineKeyboardMarkup([[InlineKeyboardButton("← Diagnostics", callback_data="s:diagnostics")]]),
        )

    elif data == "diag:overrides":
        await edit(_tp.panel_debug_overrides_text(_settings), _tp.kb_debug_overrides(_settings))

    elif str(data).startswith("dbg:mode:"):
        mode = str(data).split(":")[-1].strip().lower()
        if mode == "live":
            mode = "live_override"
        if mode not in {"off", "probe", "live_override"}:
            mode = "off"
        _settings.set_val("debug_override_mode", mode)
        await edit(_tp.panel_debug_overrides_text(_settings), _tp.kb_debug_overrides(_settings))

    elif str(data).startswith("dbg:cycle:"):
        fam = str(data).split(":")[-1].strip().lower()
        key = f"debug_override_{fam}"
        cur = str(_v(key, "normal") or "normal").strip().lower()
        order = ["normal", "relaxed", "unlimited"]
        try:
            nxt = order[(order.index(cur) + 1) % len(order)]
        except Exception:
            nxt = "normal"
        _settings.set_val(key, nxt)
        await edit(_tp.panel_debug_overrides_text(_settings), _tp.kb_debug_overrides(_settings))

    elif data == "dbg:reset":
        _settings.set_val("debug_override_mode", "off")
        for fam in (
            "global_cap",
            "engine_cap",
            "weekly_cap",
            "exposure_cap",
            "correlation_cap",
            "same_direction_cap",
            "portfolio_direction_cap",
            "lane_budget",
        ):
            _settings.set_val(f"debug_override_{fam}", "normal")
        await edit(_tp.panel_debug_overrides_text(_settings), _tp.kb_debug_overrides(_settings))

    elif data == "diag:decision_trace":
        await edit(_tp.panel_decision_trace_text(_settings), InlineKeyboardMarkup([[InlineKeyboardButton("← Diagnostics", callback_data="s:diagnostics")]]))

    elif data == "diag:api_health":
        await edit(_tp.panel_api_health_text(_settings), InlineKeyboardMarkup([[InlineKeyboardButton("← Diagnostics", callback_data="s:diagnostics")]]))

    elif data == "diag:telegram_health":
        health = _tg_backend.telegram_health_snapshot()
        lag = health.get("event_loop_lag_ms", {}) or {}
        counters = health.get("counters", {}) or {}
        critical = {
            name: row for name, row in (health.get("latency_by_action", {}) or {}).items()
            if name in {"status", "balance", "positions", "live:disarm", "live:halt", "live:emergency"}
        }
        lines = [
            "📲 *TELEGRAM CONTROL-PLANE HEALTH*",
            "",
            f"Last update: `{health.get('last_update_received_at', '') or 'none'}`",
            f"Last response: `{health.get('last_response_sent_at', '') or 'none'}`",
            f"Backlog: `{int(health.get('handler_backlog', 0) or 0)}`",
            f"Oldest pending: `{float(health.get('oldest_pending_command_age_sec', 0.0) or 0.0):.2f}s`",
            f"API failures: `{int(counters.get('telegram_api_failures_total', 0) or 0)}`",
            f"Callback failures: `{int(counters.get('telegram_callback_failures', 0) or 0)}`",
            f"Loop lag median/p95/max: `{float(lag.get('median', 0.0) or 0.0):.0f}/{float(lag.get('p95', 0.0) or 0.0):.0f}/{float(lag.get('max', 0.0) or 0.0):.0f}ms`",
            "",
            "*Safety action latency*",
        ]
        if critical:
            for name, row in critical.items():
                lines.append(
                    f"`{name}` n={int(row.get('count', 0) or 0)} "
                    f"p50 `{float(row.get('median_ms', 0.0) or 0.0):.0f}ms` "
                    f"p95 `{float(row.get('p95_ms', 0.0) or 0.0):.0f}ms` "
                    f"max `{float(row.get('max_ms', 0.0) or 0.0):.0f}ms`"
                )
        else:
            lines.append("`COLLECTING`")
        await edit("\n".join(lines), InlineKeyboardMarkup([[InlineKeyboardButton("← Diagnostics", callback_data="s:diagnostics")]]))

    elif data == "diag:signaldiag":
        cached = await _tg_backend.cached_analytical_snapshot(
            "silent_signal_diagnostics",
            lambda: _tp.render_silent_signal_diagnostics_panel(_settings),
            ttl_sec=30.0,
            provenance="silent_signal_runtime_telemetry",
        )
        await edit(
            f"{cached['value']}\n\nSnapshot: `{cached['generated_at']}` · source `{cached['provenance']}`",
            InlineKeyboardMarkup([[InlineKeyboardButton("← Diagnostics", callback_data="s:diagnostics")]]),
        )

    elif data == "diag:control_state":
        _bal = await _settings_balance()
        await edit(_tp.panel_control_state_text(_settings, balance=_bal), InlineKeyboardMarkup([[InlineKeyboardButton("← Diagnostics", callback_data="s:diagnostics")]]))

    elif data == "diag:engines":
        _bal = await _settings_balance()
        cached = await _tg_backend.cached_analytical_snapshot(
            "engine_diagnostics",
            lambda: _tp.panel_engine_diagnostics_text(_settings, balance=_bal),
            ttl_sec=30.0,
            provenance="engine_health_and_counterfactual_telemetry",
        )
        await edit(
            f"{cached['value']}\n\nSnapshot: `{cached['generated_at']}` · source `{cached['provenance']}`",
            InlineKeyboardMarkup([[InlineKeyboardButton("← Diagnostics", callback_data="s:diagnostics")]]),
        )

    elif data == "diag:shadow_rehab":
        await edit(
            _tp.panel_shadow_rehab_text(_settings),
            InlineKeyboardMarkup([[InlineKeyboardButton("← Diagnostics", callback_data="s:diagnostics")]]),
        )

    elif data == "diag:izanagi_outcomes":
        await edit(
            _tp.panel_izanagi_outcomes_text(_settings),
            InlineKeyboardMarkup([[InlineKeyboardButton("← Diagnostics", callback_data="s:diagnostics")]]),
        )

    elif data == "diag:risk":
        _bal = await _settings_balance()
        await edit(
            _tp.panel_diag_risk_text(_settings, balance=_bal),
            InlineKeyboardMarkup([[InlineKeyboardButton("← Diagnostics", callback_data="s:diagnostics")]]),
        )

    elif data == "diag:micro_sizing":
        _bal = await _settings_balance()
        await edit(
            _tp.panel_micro_sizing_text(_settings, balance=_bal),
            InlineKeyboardMarkup([[InlineKeyboardButton("← Diagnostics", callback_data="s:diagnostics")]]),
        )

    elif data == "diag:strategy":
        _bal = await _settings_balance()
        await edit(
            _tp.panel_diag_strategy_text(_settings, balance=_bal),
            InlineKeyboardMarkup([[InlineKeyboardButton("← Diagnostics", callback_data="s:diagnostics")]]),
        )

    elif data == "diag:no_trades":
        _bal = await _settings_balance()
        await edit(
            _tp.panel_no_trades_text(_settings, balance=_bal),
            InlineKeyboardMarkup([[InlineKeyboardButton("← Diagnostics", callback_data="s:diagnostics")]]),
        )

    elif data == "diag:sync":
        lines = ["🛰 *Sync Health*", "━━━━━━━━━━━━━━━━━━", ""]
        try:
            import sync_health as _sh
            snap = _sh.get_snapshot()
            lines.append(f"Overall: {snap.get('status_emoji','🟢')} `{snap.get('status','OK')}`")
            if snap.get("reason"):
                lines.append(f"Reason: `{str(snap.get('reason'))[:120]}`")
            sources = snap.get("sources", {}) or {}
            if sources:
                lines.append("")
                lines.append("*Sources:*")
                for src, s in sources.items():
                    lines.append(f"• `{src}`: `{s.get('status','OK')}` fails=`{s.get('failures',0)}` last=`{str(s.get('last_error',''))[:60]}`")
        except Exception as e:
            lines.append(f"Overall: `UNKNOWN` ({type(e).__name__})")
        await edit(
            "\n".join(lines),
            InlineKeyboardMarkup([[InlineKeyboardButton("← Diagnostics", callback_data="s:diagnostics")]]),
        )

    elif data == "diag:pnl_sync":
        snap = await _live_position_snapshot(force_refresh=True)
        await edit(_tp.panel_pnl_sync_text(snap), _tp.kb_pnl_sync(snap))

    elif str(data).startswith("diag:pnl_position:"):
        symbol = str(data).split(":", 2)[-1].upper()
        snap = await _live_position_snapshot(force_refresh=True)
        await edit(
            _tp.panel_pnl_sync_text(snap, symbol=symbol),
            InlineKeyboardMarkup([[InlineKeyboardButton("← PnL Sync", callback_data="diag:pnl_sync"),
                                   InlineKeyboardButton("← Diagnostics", callback_data="s:diagnostics")]]),
        )

    elif data == "s:help":
        await edit(_tp.panel_settings_help_text(), _tp.kb_settings_help())

    elif data == "risk:filters":
        bs = _perf.is_blackswan_halted()
        spread   = float(_v("v6_spread_max_pct", 0.30))
        vol_min  = float(_v("v6_min_volume_usd_24h", 2_000_000.0))
        spike    = float(_v("v6_vol_spike_atr_mult", 3.0))
        mkt_on   = bool(_v("v6_market_filter_enabled", True))
        corr_on  = bool(_v("v6_correlation_control_enabled", True))
        max_corr = int(_v("correlation_max_same_group", 2))
        await edit(
            f"⛔ *Active Filters*\n"
            f"━━━━━━━━━━━━━━━━━━\n\n"
            f"Market Filter:  {'🟢 ON' if mkt_on else '🔴 OFF'}\n"
            f"  Max spread:  `{spread:.2f}%`\n"
            f"  Min 24h vol: `${vol_min/1e6:.0f}M`\n"
            f"  ATR spike:   `{spike:.1f}×`\n\n"
            f"Correlation:   {'🟢 ON' if corr_on else '🔴 OFF'}\n"
            f"  Max/group:   `{max_corr}` trades\n\n"
            f"BTC Blackswan: `{'ACTIVE ⛔' if bs else 'clear 🟢'}`",
            InlineKeyboardMarkup([[InlineKeyboardButton("← Risk", callback_data="p:risk")]]),
        )

    elif data == "risk:cooldowns":
        cool = int(_v("cooldown_mins", 15))
        eng_streak = int(_v("engine_guard_loss_streak", 8))
        freq_on = bool(_v("v6_trade_frequency_enabled", True))
        max_hr = int(_v("v6_growth_max_trades_per_hour", 5))
        import time as _time
        try:
            import category_engine as _ce
            now = _time.time()
            active_cds = {c: int(u - now) for c, u in _ce._single_sl_cooldown.items() if u > now}
        except Exception:
            active_cds = {}
        cd_lines = "\n".join(f"  • `{c}` ({s}s left)" for c, s in sorted(active_cds.items())) if active_cds else "  _None active_"
        await edit(
            f"⏱️ *Cooldowns & Rate Limits*\n"
            f"━━━━━━━━━━━━━━━━━━\n\n"
            f"Per-coin cooldown:  `{cool}m`\n"
            f"Engine loss streak: `{eng_streak}` max\n"
            f"Freq control:       {'🟢 ON' if freq_on else '🔴 OFF'}\n"
            f"Max trades/hr:      `{max_hr}`\n\n"
            f"*Active coin cooldowns:*\n{cd_lines}",
            InlineKeyboardMarkup([[InlineKeyboardButton("← Risk", callback_data="p:risk")]]),
        )

    elif data == "c:reset_confirm":
        await edit(
            "🧹 *Reset Daily Counters?*\n\n"
            "Resets: daily trade count, daily PnL counters, loss streak, frequency buckets.\n"
            "_Open positions and all-time stats are NOT affected._",
            _tp.kb_confirm("c:reset_ok", "p:control"),
        )

    elif data == "c:reset_ok":
        try:
            _perf.reset_daily_gain()
            msg = "🧹 Daily counters reset."
        except Exception as e:
            msg = f"⚠️ Reset failed: {e}"
        await edit(msg, _tp.kb_control(engine.running))

    # ── Help subpanel callbacks ────────────────────────────────────────────────

    elif data == "help:v6guide":
        await edit(_tp.HELP_V6_TEXT,
                   InlineKeyboardMarkup([[InlineKeyboardButton("← Help", callback_data="help_menu")]]))

# ── AI Commands ───────────────────────────────────────────────────────────────

async def cmd_aisignal(u, ctx):
    if not is_allowed(u.effective_user.id): return await _deny(u)
    await u.message.reply_text("AI commands were removed. Use the live diagnostics and engine panels.")

async def cmd_aiscan(u, ctx):
    if not is_allowed(u.effective_user.id): return await _deny(u)
    await u.message.reply_text("AI commands were removed. Use the live diagnostics and engine panels.")

async def cmd_analyze(u, ctx):
    if not is_allowed(u.effective_user.id): return await _deny(u)
    args = ctx.args or []; coin = args[0].upper() if args else "BTC"
    tf = args[1].lower() if len(args)>1 else "15m"
    tf = {"1h":"1H","4h":"4H","1d":"1D","15":"15m","5":"5m"}.get(tf, tf)
    await ctx.bot.send_chat_action(u.effective_chat.id, "typing")
    await u.message.reply_text(f"🔬 Analysing *{coin}* ({tf})…", parse_mode="Markdown")
    try:
        result = await analyze_coin(coin, tf)
        for chunk in [result[i:i+4000] for i in range(0, len(result), 4000)]:
            await u.message.reply_text(chunk, parse_mode="Markdown")
    except Exception as e: await u.message.reply_text(f"❌ {e}")

# ── Message handler ───────────────────────────────────────────────────────────

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    uid = update.effective_user.id; text = update.message.text
    if not is_allowed(uid): return await _deny(update)

    # ── Leverage input mode ────────────────────────────────────────────────
    if chat_mode.get(uid) == "await_leverage":
        try:
            lev = max(int(_v("leverage_min", 5)), min(int(text.strip()), int(_v("leverage_max",15))))
            _settings.set_val("leverage_default", lev); chat_mode.pop(uid, None)
            await update.message.reply_text(f"📐 Leverage set to `{lev}x`", reply_markup=main_menu(), parse_mode="Markdown")
        except ValueError: await update.message.reply_text("❌ Send a number inside the live range, e.g. `20`", parse_mode="Markdown")
        return

    numeric_mode = str(chat_mode.get(uid, "") or "")
    if numeric_mode.startswith("await_numeric_setting:"):
        key = numeric_mode.split(":", 1)[1]
        if key != "minimum_net_capture_fee_multiple":
            chat_mode.pop(uid, None)
            return
        try:
            value = float(text.strip())
            if not math.isfinite(value) or value < 0.0:
                raise ValueError
        except (TypeError, ValueError):
            await update.message.reply_text(
                "❌ Enter a finite fee multiple of `0.00` or greater.",
                parse_mode="Markdown",
            )
            return
        _settings.set_val(key, round(value, 6))
        chat_mode.pop(uid, None)
        await update.message.reply_text(
            _tp.panel_room_number(
                _settings,
                key,
                "💰 Minimum Net Capture",
                "Minimum economically retained net profit required for discretionary profit capture/protection, relative to estimated transaction costs.",
                fmt="{v:.2f}× fees",
            ),
            reply_markup=_tp.kb_room_number(
                cb_dn=f"room:num:{key}:dn",
                cb_up=f"room:num:{key}:up",
                cb_custom=f"room:num:{key}:custom",
                back_feature="set:fg:profit_lock",
                back_category="set:cat:execution",
            ),
            parse_mode="Markdown",
        )
        return

    # ── Proposal size adjustment mode ─────────────────────────────────────
    if chat_mode.get(uid) == "ai_adjust_proposal":
        chat_mode.pop(uid, None)
        return

    # Non-command free text is ignored now that AI command interpretation is removed.
    return

# ── Jobs ──────────────────────────────────────────────────────────────────────

# Win rate monitor state — tracks whether an alert is currently active
# so we only fire once when it drops and once when it recovers.
_winrate_alert_active: bool = False

async def _winrate_monitor_job(ctx) -> None:
    """
    Runs every 30 minutes. Checks rolling-20 win rate across all engines.
    Sends an alert if it drops below the configured threshold.
    Sends a recovery alert when it climbs back above.
    """
    global _winrate_alert_active
    import settings_store as _s_bot
    if not _s_bot.get_val("winrate_alert_enabled", True):
        return
    threshold  = float(_s_bot.get_val("winrate_alert_threshold", 40.0))
    min_trades = int(_s_bot.get_val("winrate_alert_min_trades", 10))

    try:
        status = get_winrate_alert_status()
        wr     = status["global_rolling_wr"]
        trades = status["global_trades"]

        if trades < min_trades:
            return  # not enough data yet

        if wr < threshold and not _winrate_alert_active:
            _winrate_alert_active = True
            await alert_winrate_drop(
                global_wr       = wr,
                threshold       = threshold,
                trades_sampled  = trades,
                worst_engine    = status["worst_engine"],
                worst_engine_wr = status["worst_engine_wr"],
            )
        elif wr >= threshold and _winrate_alert_active:
            _winrate_alert_active = False
            await alert_winrate_recovery(global_wr=wr, threshold=threshold)

    except Exception as e:
        logger.debug("Win rate monitor job error: %s", e)


async def _daily_job(ctx): await engine.send_daily_summary(); await refresh_universe()

async def error_handler(update, ctx):
    err = ctx.error
    _tg_control_plane.count("telegram_errors_total")
    if isinstance(err, RetryAfter):
        _tg_control_plane.count("telegram_retries_total")
        _tg_control_plane.count("telegram_api_failures_total")
        logger.warning("Telegram rate limit: retry after %.1fs", float(getattr(err, "retry_after", 0) or 0))
        return
    if isinstance(err, (NetworkError, TimedOut)):
        _tg_control_plane.count("telegram_api_failures_total")
        logger.warning("Telegram network error: %s", err)
        return
    logger.error("Unhandled: %s", err, exc_info=err)

# ── Alias / shortcut commands ──────────────────────────────────────────────

async def cmd_stop(u, ctx):
    """Alias: /stop → same as /stopbot (more intuitive)."""
    await cmd_stopbot(u, ctx)

async def cmd_p(u, ctx):
    """Alias: /p → same as /pnl (quick P&L shortcut)."""
    await cmd_pnl(u, ctx)

async def cmd_engines(u, ctx):
    """Show engine list with status and controls."""
    if not is_allowed(u.effective_user.id): return await _deny(u)
    try: bal = await okx.get_usdt_balance()
    except: bal = 0.0
    await u.message.reply_text(
        f"🌌 *Engines*\n\nBalance: `${bal:,.4f}`\nTap engine for detail & controls.",
        reply_markup=engines_menu(), parse_mode="Markdown",
    )

async def cmd_trades(u, ctx):
    """Show production-only final and reconciling trades."""
    if not is_allowed(u.effective_user.id): return await _deny(u)
    await _tracked_reply(
        u.message,
        _production_trades_text(await _tg_backend.control_pnl_snapshot(priority="normal")),
        parse_mode="Markdown", reply_markup=main_menu(),
    )

async def cmd_engine_detail(u, ctx):
    """Per-engine shortcut: /ryuu, /raijin, /kage … shows engine detail + controls."""
    if not is_allowed(u.effective_user.id): return await _deny(u)
    name = u.message.text.split()[0][1:].upper()  # /ryuu → RYUU
    if name not in ENGINE_NAMES:
        await u.message.reply_text("Unknown engine.", reply_markup=main_menu())
        return
    await u.message.reply_text(
        _engine_text(name), reply_markup=engine_detail_kb(name), parse_mode="Markdown",
    )

# ── Application builder ───────────────────────────────────────────────────────

def _register_handlers(app: Application) -> None:
    global _winrate_alert_active
    import settings_store as _s_boot
    # Pre-seed win rate alert state on startup so we don't fire a stale alert
    # 10 minutes after every restart if WR was already below threshold.
    # The alert will only fire when WR *transitions* from OK → bad, not on cold boot
    # with pre-existing bad data.
    try:
        _boot_status = get_winrate_alert_status()
        _boot_wr     = _boot_status["global_rolling_wr"]
        _boot_trades = _boot_status["global_trades"]
        _boot_thresh = float(_s_boot.get_val("winrate_alert_threshold", 40.0))
        _boot_min    = int(_s_boot.get_val("winrate_alert_min_trades", 10))
        if _boot_trades >= _boot_min and _boot_wr < _boot_thresh:
            _winrate_alert_active = True
            logger.info(
                "WR monitor pre-seeded ACTIVE on startup (WR=%.1f%% < %.0f%% threshold) "
                "— no duplicate alert will fire for existing data.",
                _boot_wr, _boot_thresh,
            )
    except Exception as _e:
        logger.debug("WR monitor pre-seed check failed: %s", _e)

    summary_time = os.getenv("ALERT_DAILY_SUMMARY_TIME","20:00").split(":")
    app.job_queue.run_daily(_daily_job, time=dtime(hour=int(summary_time[0]), minute=int(summary_time[1]), tzinfo=timezone.utc))
    app.job_queue.run_repeating(_winrate_monitor_job, interval=1800, first=600)  # every 30 min
    for name, fn in [
        ("start",cmd_start),("help",cmd_help),("startbot",cmd_startbot),("stopbot",cmd_stopbot),
        ("status",cmd_status),("balance",cmd_balance),("positions",cmd_positions),("pnl",cmd_pnl),
        ("scan",cmd_scan),("universe",cmd_universe),("close",cmd_close),("closeall",cmd_closeall),
        ("sync",cmd_sync),("settings",cmd_settings),("clearbs",cmd_clearblackswan),("reset",cmd_reset),
        ("winrate",cmd_winrate),("v6",cmd_v6),("blocked",cmd_blocked),
        ("quality",cmd_quality),("liquidity",cmd_liquidity),("crt",cmd_crt),("bias",cmd_bias),("learning",cmd_learning),("gems",cmd_gems),
        ("signaldiag",cmd_signaldiag),
        # Aliases / shortcuts
        ("stop",cmd_stop),("p",cmd_p),("engines",cmd_engines),("trades",cmd_trades),
    ]:
        app.add_handler(CommandHandler(name, _instrument_command(name, fn)))
    # Per-engine shortcuts: /ryuu /raijin /kage /arashi … /ronin
    for _eng_name in [n.lower() for n in ENGINE_NAMES]:
        app.add_handler(CommandHandler(_eng_name, _instrument_command(_eng_name, cmd_engine_detail)))
    app.add_handler(CallbackQueryHandler(_instrument_callback(callbacks)))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    app.add_error_handler(error_handler)

def build_application(eng=None) -> Application:
    global engine
    if not TELEGRAM_TOKEN:
        raise RuntimeError("TELEGRAM_TOKEN or TELEGRAM_BOT_TOKEN not set in .env")
    if eng is not None: engine = eng
    app = (ApplicationBuilder()
               .token(TELEGRAM_TOKEN)
               .concurrent_updates(8)
               .connection_pool_size(16)
               .pool_timeout(10)
               .connect_timeout(30)
               .read_timeout(30)
               .write_timeout(30)
               .build())
    _register_handlers(app)
    logger.info("Telegram application built (V6.1 ⚡ %d engines).", len(ENGINE_NAMES))
    return app

async def _controlled_lifecycle_expiry_watchdog():
    """Disarm and stop scanning if the one-shot candidate window expires unused."""
    from controlled_lifecycle import gate as _controlled_lifecycle
    from live_state import state as _live_state
    while True:
        snapshot = _controlled_lifecycle.snapshot()
        status = str(snapshot.get("status", "") or "")
        if status == "EXPIRED":
            _live_state.disarm("controlled_lifecycle_authorization_expired")
            if engine.running:
                await engine.stop_all()
            logger.warning("Controlled lifecycle authorization expired unused; scanning stopped and live state disarmed.")
            return
        if status not in {"AUTHORIZED", "CLAIMED"}:
            return
        await asyncio.sleep(2.0)

async def _runtime_startup(app):
    import regime_detector as _regime
    import exit_manager as _em
    from controlled_lifecycle import gate as _controlled_lifecycle
    boot_balance = 0.0
    preflight = await okx.auth_preflight()
    if not bool(preflight.get("ok", False)):
        reason = str(preflight.get("reason", "private auth preflight failed") or "private auth preflight failed")
        logger.error("OKX private auth preflight failed on startup: %s", reason)
        if not PAPER_TRADING:
            from live_state import state as _live_state
            try:
                _live_state.record_preflight(False, reason)
            except Exception:
                logger.exception("Could not persist failed startup preflight state")
            raise RuntimeError(f"Refusing to start live trading: OKX private auth preflight failed: {reason}")
    else:
        logger.info("OKX private auth preflight passed on startup.")
    if not PAPER_TRADING:
        start_result = await engine.start_all()
        if not engine.running:
            raise RuntimeError(f"Production engine start failed: {start_result}")
        if _controlled_lifecycle.can_auto_arm():
            logger.warning("Controlled lifecycle validation scanning started: %s", start_result.replace("\n", " | "))
            app.create_task(_controlled_lifecycle_expiry_watchdog(), name="controlled-lifecycle-expiry")
        elif "EXECUTION_ELIGIBLE" in start_result:
            logger.warning("Armed production execution started: %s", start_result.replace("\n", " | "))
        else:
            logger.warning("Unarmed production research scanning started: %s", start_result.replace("\n", " | "))
    await _regime.start_regime_monitor("BTC")
    try:
        if not engine.running:
            await _em.start_monitor(okx, engine.engines)
        sync_msg = await engine.sync_positions(repair=True, startup=True)
        logger.info("Post-init sync complete: %s", sync_msg.replace("\n", " | "))
    except Exception as exc:
        logger.warning("Post-init sync failed: %s", exc)
    try:
        boot_balance = float(await okx.get_usdt_balance())
    except Exception as exc:
        logger.warning("Post-init balance fetch failed: %s", exc)

    await send_alert(_startup_ready_text(balance=boot_balance or engine._last_balance_usdt, sync_status="Boot sync active"))


async def _post_init(app):
    """Bring Telegram online before the production market-data warm-up."""
    global _runtime_startup_task
    await _tg_control_plane.start()
    await _tg_backend.start_operational_refresh(okx, _live_position_snapshot)
    _runtime_startup_task = asyncio.create_task(_runtime_startup(app), name="production-runtime-startup")
    logger.info("Telegram control plane online; production runtime warm-up continuing in background.")

async def _post_shutdown(app):
    global _runtime_startup_task
    import regime_detector as _regime
    import exit_manager as _em
    await _tg_backend.stop_operational_refresh()
    await _tg_backend.shutdown_control_work()
    if _runtime_startup_task and not _runtime_startup_task.done():
        _runtime_startup_task.cancel()
        try:
            await _runtime_startup_task
        except asyncio.CancelledError:
            pass
    _runtime_startup_task = None
    try:
        if engine.running:
            await engine.stop_all()
        else:
            from ws_stream import stream as _ws_stream
            await _ws_stream.stop()
    except Exception as exc:
        logger.debug("Engine shutdown cleanup failed: %s", exc)
    try:
        await _em.stop_monitor()
    except Exception as exc:
        logger.debug("Exit monitor shutdown cleanup failed: %s", exc)
    await _regime.stop_regime_monitor()
    await _tg_control_plane.stop()
    try:
        from okx_client import close_http_clients
        await close_http_clients()
    except Exception as exc:
        logger.debug("OKX HTTP client shutdown cleanup failed: %s", exc)
    try:
        from coin_universe import close_public_http_client
        await close_public_http_client()
    except Exception as exc:
        logger.debug("Public universe HTTP client shutdown cleanup failed: %s", exc)

def main() -> None:
    if not _runtime_ownership.owned:
        try:
            _runtime_ownership.acquire()
        except RuntimeOwnershipDenied as exc:
            logger.error("%s", exc)
            raise SystemExit(RUNTIME_OWNERSHIP_EXIT_CODE)
    try:
        logger.info("OxyPulse V6.1 starting | %s", "PAPER" if PAPER_TRADING else "LIVE")
        app = build_application()
        app.post_init = _post_init
        app.post_shutdown = _post_shutdown
        app.run_polling(drop_pending_updates=True, bootstrap_retries=-1)
    finally:
        _runtime_ownership.release()

if __name__ == "__main__":
    main()

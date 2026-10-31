"""


OxyPulse V6.1 Ã¢Â€Â” tg_panels.py


Premium Telegram UI: panel text builders, keyboard builders, formatting helpers.





Panel hierarchy:


  HOME  Ã¢Â†Â’  Portfolio Ã‚Â· Trades Ã‚Â· Intelligence Ã‚Â· Risk Ã‚Â· Control Ã‚Â· Settings Ã‚Â· Help





Keyboard naming convention: kb_<panel>()


Text builder convention:    panel_<name>_text(...)


Formatting helpers:         _fmt_*


"""


from __future__ import annotations





from typing import Optional
import re
import time
from datetime import datetime
from zoneinfo import ZoneInfo
from tp_ownership import UNRESOLVED, derive_tp_owner, normalize_persisted_owner, normalize_tp_mode


from telegram import InlineKeyboardMarkup, InlineKeyboardButton





# Ã¢Â”Â€Ã¢Â”Â€ Emoji + visual language Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€


E = {


    "green":    "Ã°ÂŸÂŸÂ¢", "red":     "Ã°ÂŸÂ”Â´", "yellow":  "Ã°ÂŸÂŸÂ¡", "blue":    "Ã°ÂŸÂ”Âµ",


    "bot":      "Ã¢ÂšÂ¡", "intel":   "Ã°ÂŸÂ§Â ", "shield":  "Ã°ÂŸÂ›Â¡Ã¯Â¸Â", "chart":   "Ã°ÂŸÂ“ÂŠ",


    "up":       "Ã°ÂŸÂ“Âˆ", "down":    "Ã°ÂŸÂ“Â‰", "box":     "Ã°ÂŸÂ“Â¦", "gear":    "Ã¢ÂšÂ™Ã¯Â¸Â",


    "wrench":   "Ã°ÂŸÂ”Â§", "help":    "Ã¢ÂÂ“", "refresh": "Ã°ÂŸÂ”Â„", "pause":   "Ã¢ÂÂ¸",


    "play":     "Ã¢Â–Â¶", "block":   "Ã°ÂŸÂšÂ«", "stop":    "Ã¢Â›Â”", "clock":   "Ã¢ÂÂ±",


    "link":     "Ã°ÂŸÂ”Â—", "target":  "Ã°ÂŸÂŽÂ¯", "robot":   "Ã°ÂŸÂ¤Â–", "dna":     "Ã°ÂŸÂ§Â¬",


    "signal":   "Ã°ÂŸÂ“Â¡", "history": "Ã°ÂŸÂ“Âœ", "folder":  "Ã°ÂŸÂ“Â‚", "money":   "Ã°ÂŸÂ’Â°",


    "sword":    "Ã¢ÂšÂ”Ã¯Â¸Â", "back":    "Ã°ÂŸÂ”Â™", "check":   "Ã¢ÂœÂ…", "cross":   "Ã¢ÂÂŒ",


    "warn":     "Ã¢ÂšÂ Ã¯Â¸Â", "fire":    "Ã°ÂŸÂ”Â¥", "star":    "Ã¢Â­Â", "lock":    "Ã°ÂŸÂ”Â’",


    "unlock":   "Ã°ÂŸÂ”Â“", "spark":   "Ã¢ÂœÂ¨", "trophy":  "Ã°ÂŸÂÂ†",


}





DIV = "Ã¢Â”ÂÃ¢Â”ÂÃ¢Â”ÂÃ¢Â”ÂÃ¢Â”ÂÃ¢Â”ÂÃ¢Â”ÂÃ¢Â”ÂÃ¢Â”ÂÃ¢Â”ÂÃ¢Â”ÂÃ¢Â”ÂÃ¢Â”ÂÃ¢Â”ÂÃ¢Â”ÂÃ¢Â”ÂÃ¢Â”ÂÃ¢Â”Â"





# Ã¢Â”Â€Ã¢Â”Â€ Formatting helpers Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€





def _engine_label(engine: str) -> str:


    try:


        from category_tracker import engine_display_name


        return engine_display_name(str(engine or ""))


    except Exception:


        return str(engine or "")








def _fmt_pnl(v: float) -> str:


    arrow = "Ã¢Â–Â²" if v >= 0 else "Ã¢Â–Â¼"


    return f"{arrow} `${v:+.2f}`"





def _fmt_pct(v: float, decimals: int = 1) -> str:


    return f"`{v:+.{decimals}f}%`"





def _fmt_bal(v: float) -> str:


    return f"`${v:,.2f}`"


def _fmt_money_or_dash(value, decimals: int = 2) -> str:
    try:
        if value is None:
            return "`-`"
        return f"`${float(value):+,.{decimals}f}`"
    except Exception:
        return "`-`"


def _fmt_price_or_dash(value) -> str:
    try:
        if value is None:
            return "`-`"
        return f"`${float(value):.8g}`"
    except Exception:
        return "`-`"


def _truth_source_label(row_or_snapshot: dict | None) -> str:
    if not row_or_snapshot:
        return "ðŸ”´ STALE"
    status = str(row_or_snapshot.get("status", "") or "")
    source = str(row_or_snapshot.get("source", "") or "")
    if status == "TEMPORARY_DESYNC":
        return "âš ï¸ DESYNC"
    if status == "SYNCING":
        return "âšª SYNC"
    if source == "EXCHANGE_TRUTH":
        return "ðŸŸ¢ OKX"
    if source == "ESTIMATE":
        return "ðŸŸ¡ EST"
    return "ðŸ”´ STALE"


def _truth_age(snapshot: dict | None) -> str:
    try:
        return f"{float((snapshot or {}).get('age_sec', 0.0) or 0.0):.0f}s"
    except Exception:
        return "-"





def _fmt_dd_level(level: int) -> str:


    icons = {0: "Ã°ÂŸÂŸÂ¢", 1: "Ã°ÂŸÂŸÂ¡", 2: "Ã°ÂŸÂŸÂ ", 3: "Ã°ÂŸÂ”Â´", 4: "Ã°ÂŸÂ›Â‘"}


    labels = {0: "Normal", 1: "Caution", 2: "Reduced", 3: "Restricted", 4: "PAUSED"}


    return f"{icons.get(level,'Ã¢ÂšÂª')} L{level} {labels.get(level,'?')}"





def _fmt_flag(val, on="ON", off="OFF") -> str:


    return f"`{on}`" if val else f"`{off}`"





def _fmt_mode(mode: str) -> str:


    icons = {"MICRO": "Ã°ÂŸÂŸÂ¢", "GROWTH": "Ã°ÂŸÂ”Âµ", "PRO": "Ã°ÂŸÂŸÂ£"}


    return f"{icons.get(mode,'Ã¢ÂšÂª')} `{mode}`"





def _fmt_regime(regime: str) -> str:


    icons = {"TRENDING": "Ã¢Â†Â— TRENDING", "RANGING": "Ã¢Â‡Â„ RANGING",


             "VOLATILE": "Ã¢ÂšÂ¡ VOLATILE", "DEAD": "Ã¢Â€Â” DEAD"}


    return f"`{icons.get(regime, regime)}`"





def _tog(key, settings) -> str:


    return "Ã¢ÂœÂ…" if settings.get_val(key) else "Ã¢ÂÂŒ"








def _strategy_runtime_snapshot(settings, balance: float | None = None) -> dict:


    bal = max(float(balance or 0.0), 0.0)


    base_size = float(settings.get_val("position_size_usdt", 100.0))


    lev_default = int(settings.get_val("leverage_default", 10))


    lev_min = int(settings.get_val("leverage_min", lev_default))


    lev_max = int(settings.get_val("leverage_max", lev_default))


    live_leverage = max(min(lev_default, lev_max), lev_min)


    exec_mode = str(settings.get_val("v6_execution_mode", "market")).upper()


    reduce_factor = float(settings.get_val("risk_auto_reduce_factor", 0.50) or 0.50)


    spread_max = float(settings.get_val("v6_spread_max_pct", 0.30))


    min_votes = float(settings.get_val("min_vote_score", 3.0))


    dynamic_sizing_enabled = bool(


        settings.get_val("dynamic_sizing_enabled", settings.get_val("v6_dynamic_sizing_enabled", True))


    )


    dynamic_target_min_usdt = float(settings.get_val("dynamic_target_min_usdt", 50.0) or 50.0)


    dynamic_target_max_usdt = float(settings.get_val("dynamic_target_max_usdt", 100.0) or 100.0)


    profit_target_enabled = bool(settings.get_val("profit_target_enabled", True))


    min_profit_target_usdt = float(settings.get_val("min_profit_target_usdt", 2.0) or 2.0)


    thresholds = {


        "MICRO": float(settings.get_val("v6_micro_confidence_threshold", 1.5)),


        "GROWTH": float(settings.get_val("v6_growth_confidence_threshold", 1.8)),


        "PRO": float(settings.get_val("v6_pro_confidence_threshold", 2.2)),


    }


    freq_caps = {


        "MICRO": int(settings.get_val("v6_micro_max_trades_per_hour", 8)),


        "GROWTH": int(settings.get_val("v6_growth_max_trades_per_hour", 5)),


        "PRO": int(settings.get_val("v6_pro_max_trades_per_hour", 3)),


    }





    profile_name = "STATIC"


    live_threshold = thresholds["GROWTH"]


    live_max_trades = freq_caps["GROWTH"]


    exposure_cap = 0.0


    profile_exposure_pct = 0.0


    profile_size_mult = 1.0


    profile_setting = "AUTO"


    deployment_mode = "Guarded"


    deployment_effective_mode = "Guarded"


    deployment_target_utilization_pct = 0.0


    deployment_max_positions = 0


    deployment_same_side_pct = 0.0


    deployment_total_exposure_pct = 0.0


    deployment_per_position_target_usdt = 0.0


    deployment_total_deployable_usdt = 0.0


    deployment_description = "Deployment profile unavailable"


    deployment_reference_leverage = live_leverage


    deployment_respect_exposure_cap = True


    deployment_respect_headroom = True


    if bal > 0 and settings.get_val("v6_enabled", False) and settings.get_val("v6_capital_adaptive_enabled", True):


        try:


            from capital_manager import get_profile as _get_profile


            profile = _get_profile(bal)


            profile_name = str(profile.mode or "GROWTH").upper()


            profile_setting = str(getattr(profile, "profile_setting", "AUTO") or "AUTO").upper()


            profile_size_mult = float(profile.size_multiplier or 1.0)


            live_threshold = float(profile.confidence_threshold)


            live_max_trades = int(profile.max_trades_per_hour)


            profile_exposure_pct = float(profile.max_exposure_pct)


            exposure_cap = bal * profile_exposure_pct / 100.0


            deployment_mode = str(getattr(profile, "deployment_mode", deployment_mode) or deployment_mode)


            deployment_effective_mode = str(getattr(profile, "deployment_effective_mode", deployment_mode) or deployment_mode)


            deployment_target_utilization_pct = float(


                getattr(profile, "deployment_target_utilization_pct", deployment_target_utilization_pct) or deployment_target_utilization_pct


            )


            deployment_max_positions = int(getattr(profile, "deployment_max_positions", deployment_max_positions) or deployment_max_positions)


            deployment_same_side_pct = float(


                getattr(profile, "deployment_same_side_limit_pct", deployment_same_side_pct) or deployment_same_side_pct


            )


            deployment_total_exposure_pct = float(


                getattr(profile, "deployment_total_exposure_limit_pct", deployment_total_exposure_pct) or deployment_total_exposure_pct


            )


            deployment_per_position_target_usdt = float(


                getattr(profile, "deployment_per_position_target_usdt", deployment_per_position_target_usdt) or deployment_per_position_target_usdt


            )


            deployment_total_deployable_usdt = float(


                getattr(profile, "deployment_total_deployable_usdt", deployment_total_deployable_usdt) or deployment_total_deployable_usdt


            )


            deployment_reference_leverage = int(


                getattr(profile, "deployment_reference_leverage", deployment_reference_leverage) or deployment_reference_leverage


            )


            deployment_description = str(getattr(profile, "deployment_description", deployment_description) or deployment_description)


            deployment_respect_exposure_cap = bool(


                getattr(profile, "deployment_respect_exposure_cap", deployment_respect_exposure_cap)


            )


            deployment_respect_headroom = bool(


                getattr(profile, "deployment_respect_headroom", deployment_respect_headroom)


            )


        except Exception:


            profile_name = "MICRO" if bal < float(settings.get_val("v6_micro_max_balance", 100.0)) else "GROWTH"





    direction_cap_pct = 0.0


    if profile_name in {"MICRO", "GROWTH", "PRO"}:


        profile_exposure_pct = float(


            settings.get_val(


                f"v6_{profile_name.lower()}_max_exposure_pct",


                profile_exposure_pct or 0.0,


            ) or (profile_exposure_pct or 0.0)


        )


        direction_cap_pct = float(


            settings.get_val(


                f"portfolio_same_direction_pct_{profile_name.lower()}",


                profile_exposure_pct or 0.0,


            ) or (profile_exposure_pct or 0.0)


        )


        if bal > 0 and profile_exposure_pct > 0:


            exposure_cap = bal * profile_exposure_pct / 100.0





    if deployment_total_exposure_pct > 0:


        profile_exposure_pct = deployment_total_exposure_pct


        exposure_cap = bal * deployment_total_exposure_pct / 100.0 if bal > 0 else exposure_cap


    if deployment_same_side_pct > 0:


        direction_cap_pct = deployment_same_side_pct





    try:
        import micro_sizing as _micro_sizing
    except Exception:
        _micro_sizing = None

    exec_floor = float(settings.get_effective_min_order_usdt(bal) if bal > 0 else settings.get_val("v6_min_position_size_usdt", 25.0))
    micro_sizing_active = bool(_micro_sizing.is_micro_sizing_active(bal, settings)) if _micro_sizing else False
    effective_margin_buffer = float(_micro_sizing.margin_buffer_usdt(bal, settings)) if _micro_sizing else float(settings.get_val("min_free_balance_buffer_usdt", 0.0) or 0.0)
    usable_margin = float(_micro_sizing.usable_margin_usdt(bal, settings)) if _micro_sizing else max(bal - effective_margin_buffer, 0.0)


    live_size_cap_pct = float(settings.get_val("v6_max_position_pct_of_balance", 0.03) or 0.03) * 100.0


    margin_cap = bal * (live_size_cap_pct / 100.0) * live_leverage if bal > 0 else base_size


    if deployment_per_position_target_usdt > 0 and not micro_sizing_active:


        margin_cap = max(margin_cap, deployment_per_position_target_usdt)
    elif micro_sizing_active and _micro_sizing:
        margin_cap = min(margin_cap, _micro_sizing.max_notional_for_margin(bal, live_leverage, settings))


    tp_mode = str(settings.get_val("tp_distance_mode", "balanced") or "balanced").lower()

    tp_attach = str(settings.get_val("tp_mode", "reference_only") or "reference_only").strip().lower()
    if tp_attach not in {"off", "reference_only", "exchange_tp", "hybrid"}:
        tp_attach = "reference_only"

    tp1_scale, tp2_scale = (
        settings.get_tp_distance_scales(tp_mode)


        if hasattr(settings, "get_tp_distance_scales")


        else (1.0, 1.0)


    )


    trade_intent_mode = "NORMAL"


    trade_intent_summary = "intent unavailable"


    trade_intent_size_mult = 1.0


    trade_intent_tp_mult = 1.0


    trade_intent_freq_mult = 1.0


    try:


        import trade_intent as _intent





        _intent_state = _intent.get_trade_intent(balance=bal)


        trade_intent_mode = str(_intent_state.mode or "NORMAL").upper()


        trade_intent_summary = str(_intent_state.summary or trade_intent_mode)


        trade_intent_size_mult = float(_intent_state.size_multiplier or 1.0)


        trade_intent_tp_mult = float(_intent_state.tp_multiplier or 1.0)


        trade_intent_freq_mult = float(_intent_state.frequency_multiplier or 1.0)


        if not bool(_intent_state.allow_new_trades):


            live_max_trades = 0


        else:


            live_max_trades = max(int(round(live_max_trades * max(trade_intent_freq_mult, 0.0))), 1)


    except Exception:


        pass





    if settings.get_val("v6_enabled", False):


        raw_live_limit = min(base_size, margin_cap, exposure_cap or base_size)


        projected_live_size = max(exec_floor, raw_live_limit)


    else:


        pct_size = bal * float(settings.get_val("position_size_pct", 0.05)) if bal > 0 else 0.0


        cfg_cap_pct = float(settings.get_val("position_size_cap_pct_balance", 35.0))


        cfg_cap_usdt = float(settings.get_val("position_size_cap_usdt", 250.0))


        raw_size = max(base_size, pct_size)


        raw_size = max(base_size, min(cfg_cap_usdt, raw_size))


        max_size = min(bal * (cfg_cap_pct / 100.0), cfg_cap_usdt) if bal > 0 else cfg_cap_usdt


        max_size = max(max_size, exec_floor)


        projected_live_size = max(exec_floor, min(raw_size, max_size))


        exposure_cap = max_size


        raw_live_limit = min(raw_size, max_size)





    margin_used = (projected_live_size / live_leverage) if live_leverage > 0 else projected_live_size





    clamp_reason = "base size active"


    if abs(projected_live_size - exec_floor) < 0.01 and exec_floor >= min(base_size, margin_cap, exposure_cap or base_size) - 0.01:


        clamp_reason = f"execution floor ${exec_floor:.2f}"


    elif bal > 0 and abs(projected_live_size - margin_cap) < 0.01 and margin_cap <= min(base_size, exposure_cap or margin_cap):


        clamp_reason = f"margin cap ${margin_cap:.2f}"


    elif exposure_cap and abs(projected_live_size - exposure_cap) < 0.01 and exposure_cap <= min(base_size, margin_cap):


        clamp_reason = f"profile exposure cap ${exposure_cap:.2f}"


    dynamic_band_mult = max(profile_size_mult * trade_intent_size_mult, 0.1)


    if micro_sizing_active and _micro_sizing:
        dynamic_window = _micro_sizing.target_band_usdt(bal, settings, cap_mult=dynamic_band_mult)
    else:
        dynamic_window = (
            max(min(dynamic_target_min_usdt * dynamic_band_mult, dynamic_target_max_usdt * dynamic_band_mult), exec_floor),
            max(dynamic_target_max_usdt * dynamic_band_mult, max(min(dynamic_target_min_usdt * dynamic_band_mult, dynamic_target_max_usdt * dynamic_band_mult), exec_floor)),
        )


    if dynamic_sizing_enabled:


        dynamic_min, dynamic_max = dynamic_window


        upward_cap = min(margin_cap, exposure_cap or margin_cap) if bal > 0 else max(base_size, dynamic_max)


        if projected_live_size < dynamic_min <= upward_cap:


            projected_live_size = dynamic_min


            clamp_reason = f"dynamic min ${dynamic_min:.2f}"


        elif projected_live_size > dynamic_max:


            projected_live_size = dynamic_max


            clamp_reason = f"dynamic max ${dynamic_max:.2f}"


        margin_used = (projected_live_size / live_leverage) if live_leverage > 0 else projected_live_size





    session_name = "OFF"


    try:


        from session_context import get_session as _get_session


        session_name = str(_get_session() or "OFF").upper()


    except Exception:


        session_name = "OFF"


    soft_accept_threshold = float(


        settings.get_val(


            f"soft_accept_min_confidence_{profile_name.lower()}" if profile_name in {"MICRO", "GROWTH", "PRO"} else "soft_accept_min_confidence",


            settings.get_val("soft_accept_min_confidence", 3.5),


        )


    )


    if profile_name == "MICRO" and session_name == "OFF":


        soft_accept_threshold = max(


            0.0,


            soft_accept_threshold - float(settings.get_val("soft_accept_off_session_confidence_boost_micro", 0.2)),


        )





    return {


        "balance": bal,


        "base_size": base_size,


        "lev_default": lev_default,


        "live_leverage": live_leverage,


        "lev_min": lev_min,


        "lev_max": lev_max,


        "min_votes": min_votes,


        "exec_mode": exec_mode,


        "spread_max": spread_max,


        "thresholds": thresholds,


        "freq_caps": freq_caps,


        "profile_name": profile_name,


        "projected_live_size": round(projected_live_size, 2),


        "margin_used": round(margin_used, 2),


        "live_size_cap_pct": round(live_size_cap_pct, 2),


        "margin_cap": round(margin_cap, 2),


        "profile_exposure_pct": round(profile_exposure_pct, 2),


        "direction_cap_pct": round(direction_cap_pct, 2),


        "exposure_cap": round(exposure_cap, 2),


        "live_threshold": live_threshold,


        "live_max_trades": live_max_trades,


        "clamp_reason": clamp_reason,


        "exec_floor": exec_floor,
        "micro_sizing_active": micro_sizing_active,
        "effective_account_floor_usdt": round(exec_floor, 2),
        "effective_margin_buffer_usdt": round(effective_margin_buffer, 2),
        "usable_margin_usdt": round(usable_margin, 2),


        "soft_accept_threshold": round(soft_accept_threshold, 2),


        "session_name": session_name,


        "tp_distance_mode": tp_mode.upper(),


        "tp1_distance_scale": round(tp1_scale, 2),


        "tp2_distance_scale": round(tp2_scale, 2),


        "dynamic_sizing_enabled": dynamic_sizing_enabled,


        "dynamic_target_min_usdt": round(dynamic_window[0], 2),


        "dynamic_target_max_usdt": round(dynamic_window[1], 2),


        "profit_target_enabled": profit_target_enabled,


        "min_profit_target_usdt": round(min_profit_target_usdt, 2),


        "risk_floor_preserve_enabled": bool(settings.get_val("risk_floor_preserve_enabled", True)),


        "pre_reserve_size_validation_enabled": bool(settings.get_val("pre_reserve_size_validation_enabled", True)),


        "risk_auto_reduce_factor": round(float(settings.get_val("risk_auto_reduce_factor", 0.50) or 0.50), 2),


        "trade_intent_mode": trade_intent_mode,


        "trade_intent_summary": trade_intent_summary,


        "trade_intent_size_mult": round(trade_intent_size_mult, 2),


        "trade_intent_tp_mult": round(trade_intent_tp_mult, 2),


        "trade_intent_freq_mult": round(trade_intent_freq_mult, 2),


        "profile_setting": profile_setting,


        "deployment_mode": deployment_mode,


        "deployment_effective_mode": deployment_effective_mode,


        "deployment_target_utilization_pct": round(deployment_target_utilization_pct, 2),


        "deployment_max_positions": deployment_max_positions,


        "deployment_same_side_pct": round(deployment_same_side_pct, 2),


        "deployment_total_exposure_pct": round(deployment_total_exposure_pct, 2),


        "deployment_per_position_target_usdt": round(deployment_per_position_target_usdt, 2),


        "deployment_total_deployable_usdt": round(deployment_total_deployable_usdt, 2),


        "deployment_reference_leverage": deployment_reference_leverage,


        "deployment_description": deployment_description,


        "deployment_respect_exposure_cap": deployment_respect_exposure_cap,


        "deployment_respect_headroom": deployment_respect_headroom,


    }








def _active_risk_caps(settings, balance: float | None = None) -> dict:


    snap = _strategy_runtime_snapshot(settings, balance=balance)

    mode = snap.get("profile_name") if snap.get("profile_name") in {"MICRO", "GROWTH", "PRO"} else "MICRO"


    balance_usdt = max(float(balance or 0.0), 0.0)


    exposure_pct = float(snap.get("profile_exposure_pct", 0.0) or 0.0)


    headroom_pct = float(snap.get("direction_cap_pct", exposure_pct) or exposure_pct)


    return {


        "mode": mode,


        "exposure_pct": round(exposure_pct, 2),


        "headroom_pct": round(headroom_pct, 2),


        "exposure_usdt": round(balance_usdt * exposure_pct / 100.0, 2) if balance_usdt > 0 else 0.0,


        "headroom_usdt": round(balance_usdt * headroom_pct / 100.0, 2) if balance_usdt > 0 else 0.0,


    }








def _active_exposure_policy(settings, balance: float | None = None) -> dict:


    bal = max(float(balance or 0.0), 0.0)


    selected_mode = str(settings.get_val("exposure_control_mode", "Strict") or "Strict")


    try:


        from capital_manager import get_profile as _get_profile


        from exposure_tracker import get_exposure_policy as _get_policy





        profile = _get_profile(bal)


        policy = _get_policy(bal, capital_profile=profile)


        return {


            "mode": str(policy.mode or selected_mode),


            "selected_mode": selected_mode,


            "total_pct": float(policy.total_exposure_pct or 0.0),


            "same_side_pct": float(policy.same_side_pct or 0.0),


            "max_positions": int(policy.max_positions or 0),


            "correlation_limit": int(policy.correlation_limit or 0),


            "description": str(policy.description or ""),


            "warning": str(policy.warning or ""),


            "override": bool(policy.override),


        }


    except Exception:


        caps = _active_risk_caps(settings, balance=bal)


        return {


            "mode": selected_mode,


            "selected_mode": selected_mode,


            "total_pct": float(caps.get("exposure_pct", 0.0) or 0.0),


            "same_side_pct": float(caps.get("headroom_pct", 0.0) or 0.0),


            "max_positions": int(settings.get_val("max_open_positions", 10) or 10),


            "correlation_limit": int(settings.get_val("correlation_max_same_group", 2) or 2),


            "description": EXPOSURE_MODE_EXPLANATIONS.get(selected_mode, ""),


            "warning": "",


            "override": str(selected_mode).strip().lower() == "override",


        }





# Ã¢Â”Â€Ã¢Â”Â€ Back-button helper Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€





def _back(dest: str, label: str = "Ã°ÂŸÂ”Â™ Back") -> list:


    return [InlineKeyboardButton(label, callback_data=dest)]








HOT_SCOPE_NOTE = (


    "_Hot changes affect only new trade decisions. Open positions, exit plans, "


    "sync state, and monitoring loops stay untouched._"


)





PARTICIPATION_EXPLANATIONS = {


    "soft_accept_enabled": "Allows near-valid trades through when confidence is strong enough.",


    "entry_filter_enabled": "Main quality gate for rejecting weak setups.",


    "htf_filter_enabled": "Requires higher-timeframe alignment before allowing a trade.",


    "session_filter_enabled": "Restricts trading based on session timing logic.",


    "lane_budget_enabled": "Limits over-concentration in one strategy lane.",


    "lane_override_enabled": "Allows selected lane-budget exceptions when conditions are strong.",


    "correlation_filter_enabled": "Blocks trades that are too similar to existing exposure.",


    "same_direction_headroom_enabled": "Prevents stacking too much LONG or SHORT exposure.",


    "per_engine_cap_enabled": "Limits how much one engine can dominate open risk.",


    "engine_weight_adaptation_enabled": "Lets engine influence change based on performance.",


    "confirm_path_enabled": "Converts same-coin agreement into confirmation behavior.",


    "safe_reentry_enabled": "Allows controlled re-entry after close when setup still exists.",


}





EXECUTION_EXPLANATIONS = {
    "dynamic_sizing_enabled": "Adjusts position size based on current account conditions.",


    "profit_target_enabled": "Widens take-profit distance when the projected win is too small to matter.",


    "leverage_fallback_enabled": "Retries lower leverage if the exchange rejects size at higher leverage.",


    "preflight_cooldown_enabled": "Temporarily suppresses symbols that repeatedly fail exchange admission.",


    "risk_floor_preserve_enabled": "Keeps risk-reduced trades executable when safe instead of shrinking below the exchange floor.",


    "pre_reserve_size_validation_enabled": "Rejects known non-executable trades before reserve so the registry does not churn.",


    "secure_profit_mode_enabled": "Milestone-based profit management: BE, partials, and a trailing runner (no fixed profit targets).",
    "tp_mode": "Controls whether a visible TP is attached on the exchange or shown as reference only. Trailing/milestones remain active.",
    "v6_execution_allow_limit": "Lets the router use limit orders when the execution mode allows it.",
    "v6_execution_allow_staged": "Lets the router split entries into staged submissions when enabled.",


    "breakeven_enabled": "Moves stop to entry after defined progress.",


    "partial_exit_enabled": "Takes profit in stages instead of one full exit.",


    "tsl_enabled": "Trails the stop-loss after price advances.",


}





FILTER_EXPLANATIONS = {


    "spread_filter_enabled": "Avoids coins with spreads too wide for safe execution.",


    "dead_coin_suppression_enabled": "Filters inactive or poor-quality tradable symbols.",


    "executability_penalty_enabled": "Deprioritizes symbols that recently failed exchange admission.",


    "market_cap_bonus_enabled": "Gives ranking preference to larger-cap coins without restricting the universe.",


    "small_account_large_cap_preference_enabled": "Helps small accounts favor more reliable, liquid names.",


}





AI_EXPLANATIONS = {


    "v6_ai_veto_enabled": "Lets the AI gatekeeper review marginal trades before entry.",


    "v6_telemetry_enabled": "Keeps structured decision telemetry and diagnostics flowing.",


    "automation_mutation_enabled": "Allows scheduled automation to change settings or code instead of report-only mode.",


}





ADVANCED_EXPLANATIONS = {


    "v6_enabled": "Master gate for the V6.1 trading path.",


    "v6_weighted_confidence_enabled": "Uses weighted confidence instead of raw vote count only.",


    "v6_capital_adaptive_enabled": "Switches between MICRO, GROWTH, and PRO behavior automatically.",


    "v6_smart_execution_enabled": "Uses the smart execution router instead of the simplest path.",


    "v6_market_filter_enabled": "Applies the broader market-quality filter before trade entry.",


    "v6_trade_frequency_enabled": "Respects the profile-based hourly trade pacing rules.",


}





DIAGNOSTIC_EXPLANATIONS = {


    "trade_intent_enabled": "Adapts aggression using recent win rate, drawdown, and live performance.",


    "soft_kill_switch_enabled": "Cuts size, pace, or new entries automatically when win rate degrades.",


    "engine_consensus_enabled": "Rewards coins where multiple engines align on the same direction.",


    "block_diagnostics_enabled": "Adds required, available, and suggested-fix detail to trade blocks.",


    "headroom_scaling_enabled": "Scales size down to available room before hard-blocking when possible.",


    "slot_rotation_enabled": "Allows stale, unprotected low-quality trades to be replaced only by materially stronger opportunities.",


}





EXPOSURE_MODE_EXPLANATIONS = {


    "Strict": "Keeps conservative exposure, same-side, and correlation limits for new trades.",


    "Adaptive": "Adjusts exposure tolerance using drawdown, balance, trade quality, and deployment posture.",


    "Relaxed": "Allows more capital usage and looser correlation gating while preserving core execution safety.",


    "Override": "High-risk mode that heavily relaxes exposure and correlation gating for new entries only.",


}





CONTROL_GLOSSARY = [


    "Headroom = remaining safe directional room before another position can open.",


    "Exposure = total current market notional relative to account size.",


    "Lane = strategy family grouping used to avoid clustering similar trades.",


    "Deployment Mode = how aggressively capital is deployed inside the active capital profile.",


    "Soft Accept = controlled permission for near-valid setups.",


    "Preflight = exchange admission check before order placement.",


    "Clamp = the active rule limiting live trade size right now.",


    "Probation = real trading mode with stricter observation; counted inside LIVE.",


    "Trade Intent = runtime aggression mode derived from recent performance.",


    "Consensus = aligned engines on the same coin and direction.",


    "Diagnostics = required and available room shown when a trade is blocked or scaled.",


]








def _bool_state(settings, key: str, default=False) -> bool:


    return bool(settings.get_val(key, default))








def _state_text(settings, key: str, default=False) -> str:


    return "ON" if _bool_state(settings, key, default) else "OFF"








def _state_icon(settings, key: str, default=False) -> str:


    return "Ã¢ÂœÂ…" if _bool_state(settings, key, default) else "Ã¢ÂÂŒ"








def _control_line(settings, key: str, label: str, explanation: str, default=False) -> str:


    return f"{_state_icon(settings, key, default)} {label}: `{_state_text(settings, key, default)}` Ã¢Â€Â” {explanation}"








def _fmt_compact_usd(v: float) -> str:


    value = float(v or 0.0)


    if abs(value) >= 1_000_000:


        return f"${value/1_000_000:.1f}M"


    if abs(value) >= 1_000:


        return f"${value/1_000:.0f}k"


    return f"${value:.0f}"








def _select_button(label: str, callback_data: str, active: bool) -> InlineKeyboardButton:


    marker = "Ã¢Â—Â" if active else "Ã¢Â—Â‹"


    return InlineKeyboardButton(f"{marker} {label}", callback_data=callback_data)








def _preset_label(raw: str) -> str:


    value = str(raw or "").strip().lower()


    if value == "safe":


        return "Safe"


    if value == "normal":


        return "Balanced"
    if value == "aggressive":


        return "Aggressive"


    if value == "full_deployment":


        return "Full Deployment"


    if value == "ai":


        return "Adaptive"

    if value == "adaptive":

        return "Adaptive"

    if value == "scalp":

        return "Scalp"

    return "Custom"






def _engine_mode_breakdown(settings) -> dict[str, int]:


    try:


        from category_tracker import ENGINE_NAMES


    except Exception:


        return {"live": 0, "shadow": 0, "disabled": 0, "probation": 0, "total": 0}





    counts = {"live": 0, "shadow": 0, "disabled": 0, "probation": 0, "total": len(ENGINE_NAMES)}


    for name in ENGINE_NAMES:


        mode = str(settings.get_engine_mode(name) or "disabled").lower()


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








def _engine_breakdown_text(settings) -> str:


    counts = _engine_mode_breakdown(settings)


    return (


        f"{counts['live']} LIVE Ã‚Â· {counts['shadow']} SHADOW Ã‚Â· "


        f"{counts['disabled']} DISABLED Ã‚Â· {counts['total']} TOTAL"


    )





def _refresh_back(refresh_cb: str, back_cb: str) -> list[list]:


    return [[


        InlineKeyboardButton("Ã°ÂŸÂ”Â„ Refresh", callback_data=refresh_cb),


        InlineKeyboardButton("Ã°ÂŸÂ”Â™ Back",    callback_data=back_cb),


    ]]





# Ã¢Â”Â€Ã¢Â”Â€ Confirmation keyboard Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€





def kb_confirm(ok_cb: str, cancel_cb: str,


               ok_label: str = "Ã¢ÂœÂ… Confirm",


               cancel_label: str = "Ã¢ÂÂŒ Cancel") -> InlineKeyboardMarkup:


    return InlineKeyboardMarkup([[


        InlineKeyboardButton(ok_label,     callback_data=ok_cb),


        InlineKeyboardButton(cancel_label, callback_data=cancel_cb),


    ]])





# Ã¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•Â


# HOME DASHBOARD


# Ã¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•Â





def panel_home_text(


    bal: float,


    combined_stats: dict,


    regime: str,


    session: str,


    running_count: int,


    enabled_count: int,


    open_positions: int,


    drawdown_active: bool,


    paper: bool,


    # V6.1


    capital_profile=None,


    dd_state=None,


    exposure=None,


    v6_on: bool = False,
    live_snapshot: dict | None = None,


) -> str:


    c = combined_stats


    pnl_t = float(c.get("today_pnl", 0.0))


    pnl_w = float(c.get("week_pnl", 0.0))


    wl_t  = f"{c.get('today_wins',0)}W {c.get('today_losses',0)}L"


    wl_w  = f"{c.get('week_wins',0)}W {c.get('week_losses',0)}L"


    wr_t  = float(c.get("today_wr", 0.0))





    mode_str = "Ã°ÂŸÂ“Â„ PAPER" if paper else "Ã°ÂŸÂŸÂ¢ LIVE"


    bot_dot  = "Ã¢ÂšÂ¡" if running_count > 0 else "Ã¢Â­Â•"


    eng_s    = f"{running_count}/{enabled_count}"





    lines = [


        f"Ã°ÂŸÂ“ÂŠ *OxyPulse V6.1*  Ã‚Â·  {mode_str}",


        DIV,


        f"Ã°ÂŸÂ’Â° Balance: {_fmt_bal(bal)}",


        f"Ã°ÂŸÂ“Âˆ Today PnL: {_fmt_pnl(pnl_t)}  `({wr_t:.0f}% WR)`",


        f"Ã°ÂŸÂ“Â‰ Week PnL:  {_fmt_pnl(pnl_w)}  `{wl_w}`",


    ]





    if live_snapshot is not None:
        open_pnl = live_snapshot.get("open_pnl_usdt")
        source = live_snapshot.get("source_label") or _truth_source_label(live_snapshot)
        age = _truth_age(live_snapshot)
        if open_pnl is None:
            open_line = f"Open PnL: `-` ({source}, {age})"
            net_line = "Net If Closed: `-`"
        else:
            open_line = f"Open PnL: {_fmt_pnl(float(open_pnl))} ({source}, {age})"
            net_line = f"Net If Closed: {_fmt_pnl(pnl_t + float(open_pnl))}"
        fees_today = float(c.get("today_fees", c.get("fees_today", 0.0)) or 0.0)
        lines += [
            f"Realised Today: {_fmt_pnl(pnl_t)}",
            open_line,
            net_line,
            f"Fees Today: {_fmt_money_or_dash(-abs(fees_today) if fees_today else 0.0)}",
            "Funding Today: `Unavailable`",
        ]

    # Capital mode (V6.1)


    if capital_profile:


        lines.append(f"Ã¢ÂšÂ”Ã¯Â¸Â Capital Mode: {_fmt_mode(capital_profile.mode)}  Size `{capital_profile.size_multiplier:.1f}ÃƒÂ—`")


    # Drawdown guard


    if dd_state:


        lines.append(f"Ã°ÂŸÂ›Â¡Ã¯Â¸Â DD Guard: {_fmt_dd_level(dd_state.level)}  Daily `{dd_state.daily_dd_pct:.1f}%`")


    elif drawdown_active:


        lines.append(f"Ã°ÂŸÂ›Â¡Ã¯Â¸Â DD Guard: Ã°ÂŸÂ”Â´ *ACTIVE*")


    else:


        lines.append(f"Ã°ÂŸÂ›Â¡Ã¯Â¸Â DD Guard: Ã°ÂŸÂŸÂ¢ Normal")





    # Exposure (V6.1)


    if exposure and bal > 0:


        long_pct  = exposure.long_usdt  / bal * 100 if bal else 0


        short_pct = exposure.short_usdt / bal * 100 if bal else 0


        lines.append(f"Ã°ÂŸÂ“Â¦ Exposure: Long `{long_pct:.0f}%` Ã‚Â· Short `{short_pct:.0f}%` Ã‚Â· Net `{exposure.net_exposure_pct:.0f}%`")





    lines += [


        f"{bot_dot} Engines: `{eng_s}` running  Ã‚Â·  `{open_positions}` open",


        f"Ã°ÂŸÂ§Â­ Regime: {_fmt_regime(regime)}  Ã‚Â·  _{session}_",


        "",


        f"Today: `{wl_t}`  {_fmt_pnl(pnl_t)}",


        f"V6.1: {_fmt_flag(v6_on)}",


    ]


    return "\n".join(lines)








HELP_MAIN_TEXT = "\n".join([
    "*OxyPulse Help*",
    DIV,
    "Use the buttons below to inspect commands, engines, risk controls, and the V6 runtime guide.",
])

HELP_COMMANDS_TEXT = "\n".join([
    "*Commands*",
    DIV,
    "/start - open the dashboard",
    "/help - open this help panel",
    "/status - show runtime status",
])

HELP_RISK_TEXT = "\n".join([
    "*Risk Controls*",
    DIV,
    "Position sizing, exposure, break-even, trailing, and profit-lock controls are managed from Settings.",
])

HELP_V6_TEXT = "\n".join([
    "*V6 Runtime Guide*",
    DIV,
    "The bot scans active engines, reserves eligible candidates, places OKX orders, activates the registry, and registers exit plans.",
])


def kb_help() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Commands", callback_data="help_commands"),
         InlineKeyboardButton("Risk", callback_data="help_risk")],
        [InlineKeyboardButton("V6 Guide", callback_data="help:v6guide")],
        [InlineKeyboardButton("SHOGUN", callback_data="help_shogun"),
         InlineKeyboardButton("SUSANOO", callback_data="help_susanoo")],
        [InlineKeyboardButton("FUJIN", callback_data="help_fujin"),
         InlineKeyboardButton("AMATERASU", callback_data="help_amaterasu")],
        [InlineKeyboardButton("OROCHI", callback_data="help_orochi"),
         InlineKeyboardButton("TSUNAMI", callback_data="help_tsunami")],
        [InlineKeyboardButton("SUIJIN", callback_data="help_suijin"),
         InlineKeyboardButton("RAIJUU", callback_data="help_raijuu")],
        [InlineKeyboardButton("Home", callback_data="main_menu")],
    ])


def kb_home(running: bool = False) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Dashboard", callback_data="main_menu"),
         InlineKeyboardButton("Control", callback_data="home_control")],
        [InlineKeyboardButton("Positions", callback_data="positions_view"),
         InlineKeyboardButton("Modes", callback_data="s:modes")],
        [InlineKeyboardButton("Settings", callback_data="settings_menu"),
         InlineKeyboardButton("Diagnostics", callback_data="s:diagnostics")],
        [InlineKeyboardButton("AI", callback_data="s:ai"),
         InlineKeyboardButton("Help", callback_data="help_menu")],
    ])



# Ã¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•Â


# PORTFOLIO PANEL


# Ã¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•Â





def panel_portfolio_text(


    bal: float,


    positions: dict,


    combined_stats: dict,


    capital_profile=None,


    dd_state=None,


    exposure=None,


    perf_metrics: dict | None = None,
    live_snapshot: dict | None = None,


) -> str:


    c = combined_stats


    perf_metrics = perf_metrics or {}





    open_pnl = (live_snapshot or {}).get("open_pnl_usdt")
    open_source = (live_snapshot or {}).get("source_label") or _truth_source_label(live_snapshot)





    lines = [


        "Ã°ÂŸÂ“ÂŠ *Portfolio*",


        DIV,


        f"Ã°ÂŸÂ’Â° Balance:     {_fmt_bal(bal)}",


    ]





    if live_snapshot is not None:


        if open_pnl is None:
            lines.append(f"Open PnL: `—` ({open_source}, {_truth_age(live_snapshot)})")
        else:
            lines.append(f"Open PnL: {_fmt_pnl(float(open_pnl))} ({open_source}, {_truth_age(live_snapshot)})")





    lines += [


        f"Ã°ÂŸÂ“Â‚ Open Pos:   `{len(positions)}`",


        f"Ã°ÂŸÂ“Âˆ Today PnL:  {_fmt_pnl(float(c.get('today_pnl', 0.0)))}",


        f"Ã°ÂŸÂ“Â‰ Week PnL:   {_fmt_pnl(float(c.get('week_pnl', 0.0)))}",


        f"Ã°ÂŸÂÂ† All-time:   {_fmt_pnl(float(perf_metrics.get('total_pnl', 0.0)))}",


        f"Ã¢ÂšÂ–Ã¯Â¸Â Win rate:   `{perf_metrics.get('win_rate', 0.0):.0f}%`  ({perf_metrics.get('total', 0)} trades)",


    ]





    if capital_profile:


        lines += [


            "",


            f"Ã¢ÂšÂ”Ã¯Â¸Â Mode: {_fmt_mode(capital_profile.mode)}",


            f"  RiskÃƒÂ—: `{capital_profile.risk_multiplier:.2f}`  Ã‚Â·  SizeÃƒÂ—: `{capital_profile.size_multiplier:.2f}`",


            f"  Max trades/hr: `{capital_profile.max_trades_per_hour}`",


        ]





    if dd_state and dd_state.level > 0:


        lines += ["", f"Ã¢ÂšÂ Ã¯Â¸Â DD Guard: {_fmt_dd_level(dd_state.level)}"]





    if exposure:


        long_pct  = exposure.long_usdt  / bal * 100 if bal else 0


        short_pct = exposure.short_usdt / bal * 100 if bal else 0


        lines += [


            "",


            f"Ã°ÂŸÂ“Â¦ Long:  `${exposure.long_usdt:,.0f}` (`{long_pct:.0f}%`) ÃƒÂ— {exposure.long_count} pos",


            f"Ã°ÂŸÂ“Â¦ Short: `${exposure.short_usdt:,.0f}` (`{short_pct:.0f}%`) ÃƒÂ— {exposure.short_count} pos",


        ]





    return "\n".join(lines)








def kb_portfolio() -> InlineKeyboardMarkup:


    return InlineKeyboardMarkup([


        [InlineKeyboardButton("Ã°ÂŸÂ“Â‚ Positions",    callback_data="positions_view"),


         InlineKeyboardButton("Ã°ÂŸÂ“Âœ Trade History",callback_data="p:trades")],


        [InlineKeyboardButton("Ã°ÂŸÂ“Â‰ PnL Report",   callback_data="report_combined"),


         InlineKeyboardButton("Ã°ÂŸÂ“Âˆ Win Rate",     callback_data="report_winrate")],


        *_refresh_back("p:portfolio", "main_menu"),


    ])








# Ã¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•Â


# TRADES PANEL


# Ã¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•Â





def panel_trades_text(trades: list, open_count: int = 0) -> str:
    def _trade_get(trade, key: str, default=None):
        if isinstance(trade, dict):
            return trade.get(key, default)
        return getattr(trade, key, default)


    lines = [


        "Ã°ÂŸÂ“Âˆ *Trades*",


        DIV,


        f"Ã°ÂŸÂ“Â‚ Open now: `{open_count}`",


        "",


    ]


    if not trades:


        lines.append("_No recent trades available._")


        return "\n".join(lines)





    lines.append(f"*Last {min(len(trades), 10)} closed:*\n")


    for t in reversed(trades[-10:]):


        em    = "Ã¢ÂœÂ…" if getattr(t, "pnl_usdt", 0) >= 0 else "Ã¢ÂÂŒ"


        em    = "WIN" if float(_trade_get(t, "pnl_usdt", 0.0) or 0.0) >= 0 else "LOSS"


        coin  = _trade_get(t, "coin", "?")


        dirn  = str(_trade_get(t, "direction", "?") or "?").upper()


        pnl   = float(_trade_get(t, "pnl_usdt", 0.0) or 0.0)


        eng   = _trade_get(t, "engine",  "?")


        why   = _trade_get(t, "exit_reason", "?")


        lines.append(f"  {em} `{coin}` {dirn}  `${pnl:+.4f}`  [{_engine_label(eng)}]  _{why}_")





    wins   = sum(1 for t in trades if float(_trade_get(t, "pnl_usdt", 0.0) or 0.0) >= 0)


    total  = len(trades)


    total_pnl = sum(float(_trade_get(t, "pnl_usdt", 0.0) or 0.0) for t in trades)


    lines += [


        "",


        f"Summary: `{wins}W / {total - wins}L` of `{total}`  Ã‚Â·  {_fmt_pnl(total_pnl)} total",


    ]


    return "\n".join(lines)








def panel_trades_text(trades: list, open_count: int = 0) -> str:
    """Render a compact operator-facing trade summary."""

    def _trade_get(trade, key: str, default=None):
        if isinstance(trade, dict):
            return trade.get(key, default)
        return getattr(trade, key, default)

    def _clean_coin(value) -> str:
        coin = str(value or "?").strip().upper()
        for suffix in ("-USDT-SWAP", "-USDT", "USDT"):
            if coin.endswith(suffix):
                coin = coin[: -len(suffix)]
                break
        coin = coin.strip("-_/ ") or "?"
        return coin[:10]

    def _clean_direction(value) -> str:
        direction = str(value or "").strip().upper()
        if direction in {"BUY", "LONG"}:
            return "LONG "
        if direction in {"SELL", "SHORT"}:
            return "SHORT"
        return direction[:5].ljust(5) if direction else "     "

    def _clean_reason(value) -> str:
        raw = str(value or "").strip().lower().replace("-", "_").replace(" ", "_")
        mapping = {
            "milestone_tp1": "TP1",
            "tp1": "TP1",
            "milestone_tp2": "TP2",
            "tp2": "TP2",
            "sl": "SL",
            "stop_loss": "SL",
            "sl_exchange": "SL",
            "breakeven_stop": "BE",
            "break_even_stop": "BE",
            "be": "BE",
            "sync_confirmed_close": "Closed",
            "confirmed_close": "Closed",
            "closed": "Closed",
            "trailing": "Trail",
            "trailing_stop": "Trail",
            "profit_lock": "Lock",
            "profit_lock_close": "Lock",
        }
        if raw in mapping:
            return mapping[raw]
        if "tp1" in raw:
            return "TP1"
        if "tp2" in raw:
            return "TP2"
        if "trail" in raw:
            return "Trail"
        if "lock" in raw:
            return "Lock"
        if "break" in raw or raw == "be":
            return "BE"
        if raw in {"", "?", "none", "null"}:
            return "Closed"
        if raw.startswith("sl") or raw.endswith("_sl") or "stop_loss" in raw:
            return "SL"
        words = [part for part in raw.split("_") if part]
        return " ".join(part.capitalize() for part in words[:2]) or "Closed"

    def _pnl_text(value: float, neutral: bool = False) -> str:
        if neutral:
            return "$0.00"
        sign = "+" if value > 0 else "-"
        return f"{sign}${abs(value):.2f}"

    def _entry_line(item) -> str:
        suffix = f" · {item['closed_label']}" if item.get("closed_label") else ""
        if item.get("duration_label"):
            suffix += f" · {item['duration_label']}"
        return (
            f"`{item['coin']}` {item['direction']} "
            f"{item['pnl'].rjust(7)}   ({item['reason']}){suffix}"
        )

    recent = list(trades or [])[-20:]
    entries = []
    for trade in reversed(recent):
        pnl_source = _trade_get(trade, "net_pnl_usdt", None)
        if pnl_source is None:
            pnl_source = _trade_get(trade, "pnl_usdt", 0.0)
        pnl = float(pnl_source or 0.0)
        neutral = abs(pnl) < 0.01
        coin = _clean_coin(_trade_get(trade, "coin", _trade_get(trade, "symbol", "?")))
        direction = _clean_direction(_trade_get(trade, "direction", _trade_get(trade, "side", "")))
        reason = _clean_reason(_trade_get(trade, "exit_reason", _trade_get(trade, "reason", "")))
        closed_label = ""
        duration_label = ""
        try:
            import settings_store as _settings_store
            zone = ZoneInfo(str(_settings_store.get_val("operator_timezone", "Africa/Johannesburg")))
            closed_raw = str(_trade_get(trade, "closed_at", "") or "")
            if closed_raw:
                closed_dt = datetime.fromisoformat(closed_raw.replace("Z", "+00:00")).astimezone(zone)
                closed_label = closed_dt.strftime("%d %b %H:%M")
            duration_seconds = _trade_get(trade, "duration_seconds", None)
            if duration_seconds is None:
                duration_seconds = float(_trade_get(trade, "holding_minutes", 0.0) or 0.0) * 60.0
            if float(duration_seconds or 0.0) > 0:
                mins = int(round(float(duration_seconds) / 60.0))
                duration_label = f"{mins // 60}h{mins % 60:02d}m" if mins >= 60 else f"{mins}m"
        except Exception:
            pass
        entries.append({
            "coin": coin,
            "direction": direction,
            "pnl_value": pnl,
            "pnl": _pnl_text(pnl, neutral=neutral),
            "reason": reason,
            "closed_label": closed_label,
            "duration_label": duration_label,
            "bucket": "neutral" if neutral else ("wins" if pnl > 0 else "losses"),
        })

    wins = sum(1 for item in entries if item["bucket"] == "wins")
    losses = sum(1 for item in entries if item["bucket"] == "losses")
    neutral = sum(1 for item in entries if item["bucket"] == "neutral")
    net = sum(item["pnl_value"] for item in entries)
    last_label = f"{wins}W / {losses}L" + (f" / {neutral}N" if neutral else "")
    net_label = _pnl_text(net) if abs(net) >= 0.01 else "$0.00"

    lines = [
        "ðŸ“Š *Trades Overview*",
        DIV,
        f"ðŸ“‚ Open: `{open_count}`",
        f"ðŸ“ˆ Last 20: `{last_label}`",
        f"ðŸ’° Net: `{net_label}`",
        "",
    ]

    if not entries:
        lines.append("No recent trades available.")
        return "\n".join(lines)

    def _section(title: str, bucket: str) -> None:
        rows = [item for item in entries if item["bucket"] == bucket]
        if not rows:
            return
        lines.extend(["", title])
        seen = set()
        shown = 0
        hidden_dupes = 0
        for item in rows:
            key = (item["coin"], item["direction"], item["pnl"], item["reason"])
            if key in seen:
                hidden_dupes += 1
                continue
            seen.add(key)
            lines.append(_entry_line(item))
            shown += 1
            if shown >= 5:
                break
        if hidden_dupes:
            lines.append(f"+{hidden_dupes} duplicate{'s' if hidden_dupes != 1 else ''} collapsed")

    _section("ðŸŸ¢ *Recent Wins*", "wins")
    _section("ðŸ”´ *Recent Losses*", "losses")
    _section("ðŸŸ¡ *Neutral / Tiny*", "neutral")

    return "\n".join(lines)


def kb_trades() -> InlineKeyboardMarkup:


    return InlineKeyboardMarkup([


        [InlineKeyboardButton("Ã°ÂŸÂ“Â‚ Open Positions", callback_data="positions_view"),


         InlineKeyboardButton("Ã°ÂŸÂ“ÂŠ PnL Report",     callback_data="report_combined")],


        [InlineKeyboardButton("Ã°ÂŸÂÂ† Win Rate",       callback_data="report_winrate"),


         InlineKeyboardButton("Ã¢ÂšÂ™Ã¯Â¸Â Engines",        callback_data="engines_menu")],


        *_refresh_back("p:trades", "main_menu"),


    ])








# Ã¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•Â


# INTELLIGENCE PANEL


# Ã¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•Â





def panel_intel_text(


    regime: str,


    session: str,


    capital_profile=None,


    dd_state=None,


    weights: dict | None = None,


    freq_used_hr: int = 0,


    freq_max_hr: int = 8,


    freq_used_day: int = 0,


    freq_max_day: int = 30,


    rejection_summary: dict | None = None,


) -> str:


    lines = [


        "Ã°ÂŸÂ§Â  *Intelligence*",


        DIV,


        f"Ã°ÂŸÂ§Â­ Regime:   {_fmt_regime(regime)}",


        f"Ã°ÂŸÂ•Â°Ã¯Â¸Â Session:  _{session}_",


    ]





    if capital_profile:


        lines += [


            "",


            f"Ã¢ÂšÂ”Ã¯Â¸Â Capital Mode: {_fmt_mode(capital_profile.mode)}",


            f"  Confidence threshold: `{capital_profile.confidence_threshold:.1f}`",


            f"  Aggressiveness: `{capital_profile.aggressiveness_score:.0%}`",


        ]





    if dd_state:


        lines += [


            "",


            f"Ã°ÂŸÂ›Â¡Ã¯Â¸Â Drawdown Guard: {_fmt_dd_level(dd_state.level)}",


            f"  Daily DD: `{dd_state.daily_dd_pct:.1f}%`  Ã‚Â·  SizeÃƒÂ—: `{dd_state.size_multiplier:.2f}`",


        ]





    lines += [


        "",


        f"Ã¢ÂÂ±Ã¯Â¸Â Trade frequency: `{freq_used_hr}/{freq_max_hr}` this hour  Ã‚Â·  `{freq_used_day}/{freq_max_day}` today",


    ]





    if weights:


        non_default = {k: v for k, v in weights.items() if abs(float(v) - 1.0) > 0.02}


        if non_default:


            lines += ["", "*Engine weight deviations:*"]


            for eng, w in sorted(non_default.items(), key=lambda x: -abs(float(x[1]) - 1.0))[:6]:


                arrow = "Ã¢Â–Â²" if float(w) > 1.0 else "Ã¢Â–Â¼"


                lines.append(f"  {arrow} `{_engine_label(eng)}` Ã¢Â†Â’ `{float(w):.2f}`")


        else:


            lines.append("  All engine weights at default `1.0`")





    if rejection_summary:


        total = sum(rejection_summary.values())


        parts = "  Ã‚Â·  ".join(


            f"`{tag}ÃƒÂ—{n}`" for tag, n in list(rejection_summary.items())[:5]


        )


        lines += ["", f"Ã°ÂŸÂšÂ« Blocked ({total}): {parts}"]





    return "\n".join(lines)








def panel_blocked_text(rejections: list) -> str:


    """Grouped blocked-candidate log panel.





    Groups identical candidates (same engine/coin/dir/conf/tag) to reduce noise.


    Shows max 10 grouped rows, newest first.  Raw logs are preserved in rejection_log.


    """


    from collections import OrderedDict





    # Human-readable tag labels


    _TAG_LABELS = {


        "entry_filter":   "Entry Filter",


        "reserve":        "Reserve",


        "exposure":       "Exposure",


        "market_filter":  "Market Filter",


        "frequency":      "Frequency",


        "low_confidence": "Low Conf",


        "correlation":    "Correlation",


        "engine_cap":     "Engine Cap",


        "global_cap":     "Global Cap",


        "portfolio":      "Portfolio",


        "drawdown":       "Drawdown",


        "blackswan":      "Blackswan",


        "weekly_cap":     "Weekly Cap",


        "daily_cap":      "Daily Cap",


        "open_risk":      "Open Risk",


        "ai_gate":        "Review Gate",


        "sizing":         "Sizing",


        "cooldown":       "Cooldown",


        "headroom":       "Headroom",


        "lane_budget":    "Lane Budget",


        "soft_accept":    "Soft Accept Ã¢ÂœÂ…",


    }





    lines = ["Ã¢Â›Â” *Blocked Candidates*", DIV]


    if not rejections:


        lines.append("_No rejections recorded yet._")


        return "\n".join(lines)





    # Summary by tag (use friendly labels)


    summary: dict[str, int] = {}


    for r in rejections:


        summary[r.tag] = summary.get(r.tag, 0) + 1


    tag_line = "  Ã‚Â·  ".join(


        f"`{_TAG_LABELS.get(t, t)}ÃƒÂ—{n}`"


        for t, n in sorted(summary.items(), key=lambda x: -x[1])


    )


    lines += [f"*Summary:* {tag_line}", ""]





    # Group by (engine, coin, direction, conf_rounded, tag) Ã¢Â€Â” ordered by insertion (oldest first)
    _groups: dict = OrderedDict()
    for r in rejections:
        key = (r.engine, r.coin, r.direction, round(r.conf, 1), r.tag)
        if key not in _groups:
            _groups[key] = {"count": 0, "first": r.ts, "last": r.ts, "detail": ""}
        g = _groups[key]
        g["count"] += 1
        g["last"] = r.ts  # update to latest occurrence
        g["detail"] = str(getattr(r, "detail", "") or "")



    # Show max 10 groups, newest-insertion first


    group_items = list(reversed(list(_groups.items())))[:10]


    for (engine, coin, direction, conf, tag), g in group_items:


        label  = _TAG_LABELS.get(tag, tag)


        arrow  = "Ã¢Â–Â²" if direction == "LONG" else "Ã¢Â–Â¼"


        count  = g["count"]


        t_first = g["first"]


        t_last  = g["last"]


        # Show count and time range only when there are multiple occurrences


        if count > 1:


            count_str = f" {count}ÃƒÂ— ({t_first}Ã¢Â€Â“{t_last})"


        else:


            count_str = f" ({t_first})"


        lines.append(f"Ã¢Â›Â” {_engine_label(engine)} | {arrow}{direction} | {coin} | c={conf:.1f} | {label}{count_str}")

        # If probe/override annotations exist, show one compact hint line.
        d = str(g.get("detail", "") or "")
        if d and ("PROBE" in d or "override" in d.lower() or "debug_override" in d.lower()):
            d = d.replace("\n", " ").strip()
            if len(d) > 110:
                d = d[:110].rstrip() + "Ã¢Â€Â¦"
            lines.append(f"  {d}")



    return "\n".join(lines)








def kb_intel() -> InlineKeyboardMarkup:


    return InlineKeyboardMarkup([


        [InlineKeyboardButton("Ã¢ÂšÂ–Ã¯Â¸Â Engine Weights", callback_data="intel:weights"),


         InlineKeyboardButton("Ã°ÂŸÂ“ÂŠ Confidence",     callback_data="v6_status")],


        [InlineKeyboardButton("Ã°ÂŸÂšÂ« Blocked Log",    callback_data="intel:blocked"),


         InlineKeyboardButton("Ã°ÂŸÂ¤Â– AI Scan",        callback_data="aiscan_quick")],


        [InlineKeyboardButton("Ã°ÂŸÂŒÂ Universe",       callback_data="universe_quick"),


         InlineKeyboardButton("Ã°ÂŸÂ”Â„ Refresh",        callback_data="p:intel")],


        [InlineKeyboardButton("Ã°ÂŸÂ”Â™ Home",           callback_data="main_menu")],


    ])








def panel_weights_text(weights: dict) -> str:


    lines = ["Ã¢ÂšÂ–Ã¯Â¸Â *Engine Weights*", DIV]


    if not weights:


        lines.append("_No weight data Ã¢Â€Â” adaptation disabled or no trades yet._")


        return "\n".join(lines)


    sorted_w = sorted(weights.items(), key=lambda x: -float(x[1]))


    for eng, w in sorted_w:


        bar = "Ã¢Â–Â²" if float(w) > 1.05 else ("Ã¢Â–Â¼" if float(w) < 0.95 else "Ã¢Â•Â")


        lines.append(f"  {bar} `{_engine_label(eng):<18}` `{float(w):.3f}`")


    return "\n".join(lines)








# Ã¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•Â


# RISK PANEL


# Ã¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•Â





def panel_risk_text(


    bal: float,


    dd_state=None,


    exposure=None,


    capital_profile=None,


    lane_statuses: list | None = None,


    daily_cap_pct: float = 12.0,


    weekly_cap_pct: float = 25.0,


    cooldown_mins: int = 15,


    settings=None,


    freq_used_hr: int = 0,


    freq_max_hr: int = 8,


    freq_used_day: int = 0,


    freq_max_day: int = 30,


) -> str:


    lines = ["Ã°ÂŸÂ›Â¡Ã¯Â¸Â *Risk Status*", DIV]





    # Drawdown


    if dd_state:


        lines += [


            f"Ã°ÂŸÂ“Â‰ Daily DD:   `{dd_state.daily_dd_pct:.1f}%`  (cap: `{daily_cap_pct:.0f}%`)",


            f"Ã°ÂŸÂ“Â‰ Weekly DD:  `{dd_state.weekly_dd_pct:.1f}%`  (cap: `{weekly_cap_pct:.0f}%`)",


            f"Ã°ÂŸÂ›Â¡Ã¯Â¸Â Guard:      {_fmt_dd_level(dd_state.level)}",


            f"  Size mult: `{dd_state.size_multiplier:.2f}ÃƒÂ—`  Ã‚Â·  Allow new: `{dd_state.allow_new_trades}`",


        ]


    else:


        lines += [


            f"Ã°ÂŸÂ“Â‰ Daily cap:  `{daily_cap_pct:.0f}%`",


            f"Ã°ÂŸÂ“Â‰ Weekly cap: `{weekly_cap_pct:.0f}%`",


        ]





    # Exposure


    if exposure and bal > 0:


        long_pct  = exposure.long_usdt  / bal * 100 if bal else 0


        short_pct = exposure.short_usdt / bal * 100 if bal else 0


        net_pct   = exposure.net_exposure_pct


        lines += [


            "",


            f"Ã°ÂŸÂ“Â¦ Long:  `${exposure.long_usdt:,.0f}` ({long_pct:.0f}%)  Ã‚Â·  {exposure.long_count} positions",


            f"Ã°ÂŸÂ“Â¦ Short: `${exposure.short_usdt:,.0f}` ({short_pct:.0f}%)  Ã‚Â·  {exposure.short_count} positions",


            f"Ã°ÂŸÂ“Â¦ Net:   `{net_pct:.1f}%` of balance",


        ]





    if lane_statuses:


        lines += ["", "Ã°ÂŸÂ§Â­ Lane budgets:"]


        for status in lane_statuses[:5]:


            lane_name = str(status.lane).replace("_", " ").title()


            lines.append(


                f"  `{lane_name}` {status.used_pct:.0f}% / {status.budget_pct:.0f}%"


                f"  Ã‚Â·  {status.positions} pos"


            )





    # Frequency


    lines += [


        "",


        f"Ã¢ÂÂ±Ã¯Â¸Â Trade rate: `{freq_used_hr}/{freq_max_hr}` this hour  Ã‚Â·  `{freq_used_day}/{freq_max_day}` today",


    ]





    # Filters (from settings)


    if settings:


        corr_on   = bool(settings.get_val("v6_correlation_control_enabled", True))


        mkt_on    = bool(settings.get_val("v6_market_filter_enabled", True))


        freq_on   = bool(settings.get_val("v6_trade_frequency_enabled", True))


        spread    = float(settings.get_val("v6_spread_max_pct", 0.30))


        min_vol   = float(settings.get_val("v6_min_volume_usd_24h", 2_000_000.0))


        max_corr  = int(settings.get_val("correlation_max_same_group", 2))


        lines += [


            "",


            f"Ã°ÂŸÂ”Â— Correlation: {'Ã°ÂŸÂŸÂ¢ ON' if corr_on else 'Ã°ÂŸÂ”Â´ OFF'}  max `{max_corr}` same-group trades",


            f"Ã°ÂŸÂ“Â¡ Mkt Filter:  {'Ã°ÂŸÂŸÂ¢ ON' if mkt_on else 'Ã°ÂŸÂ”Â´ OFF'}  spread Ã¢Â‰Â¤`{spread:.2f}%`  vol Ã¢Â‰Â¥`${min_vol/1e6:.0f}M`",


            f"Ã¢ÂÂ±Ã¯Â¸Â Freq Ctrl:   {'Ã°ÂŸÂŸÂ¢ ON' if freq_on else 'Ã°ÂŸÂ”Â´ OFF'}",


        ]





    # Cooldown


    lines.append(f"Ã¢ÂÂ±Ã¯Â¸Â Cooldown:   `{cooldown_mins}m` per coin")





    return "\n".join(lines)








def kb_risk() -> InlineKeyboardMarkup:


    return InlineKeyboardMarkup([


        [InlineKeyboardButton("Ã°ÂŸÂ“Â‰ Drawdown",    callback_data="risk:drawdown"),


         InlineKeyboardButton("Ã°ÂŸÂ“Â¦ Exposure",   callback_data="risk:exposure")],


        [InlineKeyboardButton("Ã¢Â›Â” Filters",    callback_data="risk:filters"),


         InlineKeyboardButton("Ã¢ÂÂ±Ã¯Â¸Â Cooldowns",  callback_data="risk:cooldowns")],


        *_refresh_back("p:risk", "main_menu"),


    ])








def panel_drawdown_text(dd_state, capital_profile=None,


                        daily_cap: float = 12.0, weekly_cap: float = 25.0) -> str:


    if not dd_state:


        return "Ã°ÂŸÂ›Â¡Ã¯Â¸Â *Drawdown Guard*\n\n_No data available._"


    tol = getattr(dd_state, "tolerance_pct", daily_cap)


    lines = [


        "Ã°ÂŸÂ“Â‰ *Drawdown Detail*",


        DIV,


        f"Guard level:   {_fmt_dd_level(dd_state.level)}",


        f"Daily DD:      `{dd_state.daily_dd_pct:.2f}%`  (tolerance: `{tol:.1f}%`)",


        f"Weekly DD:     `{dd_state.weekly_dd_pct:.2f}%`  (cap: `{weekly_cap:.1f}%`)",


        f"Consec losses: `{dd_state.consec_losses}`",


        f"Size mult:     `{dd_state.size_multiplier:.2f}ÃƒÂ—`",


        f"Allow new:     `{dd_state.allow_new_trades}`",


    ]


    if capital_profile:


        lines += [


            "",


            f"Mode tolerance: {_fmt_mode(capital_profile.mode)} `{capital_profile.drawdown_tolerance_pct:.1f}%`",


            f"  L1 triggers at `{capital_profile.drawdown_tolerance_pct * 0.25:.1f}%`",


            f"  L4 (PAUSED) at `{capital_profile.drawdown_tolerance_pct:.1f}%`",


        ]


    return "\n".join(lines)








def panel_exposure_text(exposure, bal: float) -> str:


    if not exposure:


        return "Ã°ÂŸÂ“Â¦ *Exposure*\n\n_No exposure data available._"


    long_pct  = exposure.long_usdt  / bal * 100 if bal else 0


    short_pct = exposure.short_usdt / bal * 100 if bal else 0


    lines = [


        "Ã°ÂŸÂ“Â¦ *Exposure Detail*",


        DIV,


        f"Long:   `${exposure.long_usdt:,.2f}` (`{long_pct:.1f}%`)  Ã‚Â·  {exposure.long_count} positions",


        f"Short:  `${exposure.short_usdt:,.2f}` (`{short_pct:.1f}%`)  Ã‚Â·  {exposure.short_count} positions",


        f"Net:    `${exposure.net_usdt:,.2f}` (`{exposure.net_exposure_pct:.1f}%`)",


        f"Total:  `${exposure.total_usdt:,.2f}`",


    ]


    if exposure.by_group:


        lines += ["", "*By correlation group:*"]


        for grp, counts in exposure.by_group.items():


            parts = []


            if counts.get("long"): parts.append(f"LÃƒÂ—{counts['long']}")


            if counts.get("short"): parts.append(f"SÃƒÂ—{counts['short']}")


            if parts:


                lines.append(f"  `{grp}`: {' Ã‚Â· '.join(parts)}")


    return "\n".join(lines)








# Ã¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•Â


# CONTROL PANEL


# Ã¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•Â





def panel_control_text(


    running: bool,


    running_count: int,


    enabled_count: int,


    capital_profile=None,


    v6_on: bool = False,


    ai_gate: bool = False,


    engine_adapt: bool = False,


    exec_mode: str = "market",


    freq_used_hr: int = 0,


    freq_max_hr: int = 8,


    freq_used_day: int = 0,


    freq_max_day: int = 30,


    preset: str = "",


) -> str:


    status = "Ã°ÂŸÂŸÂ¢ *ACTIVE*" if running else "Ã°ÂŸÂ”Â´ *STOPPED*"


    lines = [


        "Ã¢ÂšÂ™Ã¯Â¸Â *Control Center*",


        DIV,


        f"Bot:     {status}  Ã‚Â·  `{running_count}/{enabled_count}` engines",


        f"V6.1:    {_fmt_flag(v6_on)}",


    ]


    if capital_profile:


        lines.append(f"Mode:    {_fmt_mode(capital_profile.mode)}")





    lines += [


        f"Review Gate: {_fmt_flag(ai_gate)}  Â·  Adapt: {_fmt_flag(engine_adapt)}",


        f"Exec:    `{exec_mode.upper()}`",


        f"Ã¢ÂÂ±Ã¯Â¸Â Rate:  `{freq_used_hr}/{freq_max_hr}` hr  Ã‚Â·  `{freq_used_day}/{freq_max_day}` day",


    ]


    if preset:


        preset_map = {


            "safe": "Ã°ÂŸÂ”Â’ SAFE",


            "aggressive": "Ã°ÂŸÂ”Â¥ AGGRESSIVE",


            "normal": "Ã¢Â†Â©Ã¯Â¸Â NORMAL",


            "ai": "Ã°ÂŸÂ¤Â– AI MODE",


        }


        lines.append(f"Preset:  `{preset_map.get(preset, preset.upper())}` active")





    return "\n".join(lines)








def kb_control(running: bool) -> InlineKeyboardMarkup:
    try:
        from live_state import state as _live_state
        armed = bool(_live_state.snapshot().get("armed"))
    except Exception:
        armed = False
    toggle_btn = (
        InlineKeyboardButton("🚨 DISARM NOW", callback_data="c:pause_confirm")
        if armed else
        InlineKeyboardButton("✅ ARM LIVE", callback_data="c:resume")
    )


    return InlineKeyboardMarkup([


        [toggle_btn,


         InlineKeyboardButton("Ã°ÂŸÂ”Â„ Sync",          callback_data="sync_positions")],


        [InlineKeyboardButton("Ã¢ÂšÂ™Ã¯Â¸Â Settings",      callback_data="settings_menu")],


        [InlineKeyboardButton("Ã°ÂŸÂ§Â¹ Reset Counters", callback_data="c:reset_confirm"),


         InlineKeyboardButton("Ã¢ÂšÂ™Ã¯Â¸Â V6.1 Status",   callback_data="v6_status")],


        *_refresh_back("p:control", "main_menu"),


    ])








# Ã¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•Â


# SETTINGS PANELS


# Ã¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•ÂÃ¢Â•Â





def kb_settings_main(settings) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([


        [InlineKeyboardButton("Ã°ÂŸÂŽÂ› Modes", callback_data="s:modes"),


         InlineKeyboardButton("Ã°ÂŸÂ“Â Strategy", callback_data="s:strategy")],


        [InlineKeyboardButton("Ã°ÂŸÂ›Â¡Ã¯Â¸Â Risk & Exposure", callback_data="s:risk_cfg"),


         InlineKeyboardButton("Ã°ÂŸÂ“Â¡ Execution", callback_data="s:execution")],


        [InlineKeyboardButton("Ã°ÂŸÂ§Âª Filters & Market Quality", callback_data="s:filters"),
         InlineKeyboardButton("Ã¢ÂšÂ™Ã¯Â¸Â Participation", callback_data="s:participation")],


        [InlineKeyboardButton("Ã°ÂŸÂ§Â· Slots & Allocation", callback_data="s:slots"),
         InlineKeyboardButton("Ã°ÂŸÂ§Âª Diagnostics", callback_data="s:diagnostics")],
        [InlineKeyboardButton("Ã°ÂŸÂ§Â  Engines", callback_data="s:engines"),
         InlineKeyboardButton("Ã°ÂŸÂ¤Â– AI & Automation", callback_data="s:ai")],


        [InlineKeyboardButton("Ã°ÂŸÂ”Â¬ Advanced", callback_data="s:advanced"),


         InlineKeyboardButton("Ã¢ÂÂ“ Help / Glossary", callback_data="s:help")],


        [InlineKeyboardButton("Ã°ÂŸÂ§Â° System", callback_data="s:system")],

        [InlineKeyboardButton("Ã¢Â†Âº Reset Defaults", callback_data="stg_reset_confirm")],
        [InlineKeyboardButton("Ã°ÂŸÂ”Â™ Home",                                 callback_data="main_menu")],


    ])








def panel_settings_hub_text(settings) -> str:
    return "\n".join([
        "Ã¢ÂšÂ™Ã¯Â¸Â *Settings*",
        DIV,
        "Category-first control plane.",
        "",
        "_Changes apply to NEW decisions unless the room says otherwise._",
    ])


def kb_settings_hub(settings) -> InlineKeyboardMarkup:
    # Navigational hub only (no mixed toggles here).
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("Ã°ÂŸÂ“ÂŠ Trading", callback_data="set:cat:trading"),
            InlineKeyboardButton("Ã°ÂŸÂ§Â  Strategy", callback_data="set:cat:strategy"),
        ],
        [
            InlineKeyboardButton("Ã°ÂŸÂ›Â¡Ã¯Â¸Â Risk", callback_data="set:cat:risk"),
            InlineKeyboardButton("Ã°ÂŸÂ“Â¦ Execution", callback_data="set:cat:execution"),
        ],
        [
            InlineKeyboardButton("Ã°ÂŸÂ“Â‰ Capital", callback_data="set:cat:capital"),
            InlineKeyboardButton("Control State", callback_data="diag:control_state"),
        ],
        [
            InlineKeyboardButton("Ã°ÂŸÂ§Âª Diagnostics", callback_data="set:cat:diagnostics"),
            InlineKeyboardButton("Ã°ÂŸÂŽÂ›Ã¯Â¸Â System", callback_data="set:cat:system"),
        ],
        [InlineKeyboardButton("Ã¢Â†Â©Ã¯Â¸Â Back", callback_data="main_menu")],
    ])


def panel_settings_cat_trading_text(settings) -> str:
    return "\n".join([
        "Ã°ÂŸÂ“ÂŠ *Settings Ã‚Â· Trading*",
        DIV,
        "Slots, allocation, and flow controls.",
    ])


def kb_settings_cat_trading(settings) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Ã°ÂŸÂ“Â¦ Slots & Allocation", callback_data="set:fg:slots")],
        [InlineKeyboardButton("Ã°ÂŸÂ§Â  Engine Caps", callback_data="set:fg:engine_caps")],
        [InlineKeyboardButton("Ã¢Â†Â Settings", callback_data="settings_menu")],
    ])


def panel_settings_cat_strategy_text(settings) -> str:
    return "\n".join([
        "Ã°ÂŸÂ§Â  *Settings Ã‚Â· Strategy*",
        DIV,
        "Signal quality and selection behavior.",
    ])


def kb_settings_cat_strategy(settings) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Ã°ÂŸÂŽÂ¯ Selection Policy", callback_data="set:room:trade_selection_policy")],
        [InlineKeyboardButton("Ã°ÂŸÂ§Â® Confidence Thresholds", callback_data="set:fg:confidence")],
        [InlineKeyboardButton("â˜€ï¸ AMATERASU Macro", callback_data="set:fg:amaterasu_macro")],
        [InlineKeyboardButton("Ã¢ÂšÂ–Ã¯Â¸Â Engine Weights", callback_data="set:room:engine_weight_adaptation_enabled")],
        [InlineKeyboardButton("Ã¢Â†Â Settings", callback_data="settings_menu")],
    ])


def panel_settings_cat_risk_text(settings) -> str:
    return "\n".join([
        "Ã°ÂŸÂ›Â¡Ã¯Â¸Â *Settings Ã‚Â· Risk*",
        DIV,
        "Drawdown protection and exposure policy.",
    ])


def kb_settings_cat_risk(settings) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Ã°ÂŸÂ›Â¡Ã¯Â¸Â Weekly Cap", callback_data="set:fg:weekly_cap")],
        [InlineKeyboardButton("Ã°ÂŸÂ§Â¯ Risk Ceiling", callback_data="set:fg:risk_ceiling")],
        [InlineKeyboardButton("Ã°ÂŸÂ©Â¹ Short Rehab", callback_data="set:fg:short_rehab")],
        [InlineKeyboardButton("Ã°ÂŸÂ“Â‰ Exposure", callback_data="set:fg:exposure")],
        [InlineKeyboardButton("Ã¢Â†Â Settings", callback_data="settings_menu")],
    ])


def panel_settings_cat_execution_text(settings) -> str:
    return "\n".join([
        "Ã°ÂŸÂ“Â¦ *Settings Ã‚Â· Execution*",
        DIV,
        "Execution router, exits, and market-quality filters.",
    ])


def kb_settings_cat_execution(settings) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Ã°ÂŸÂŽÂ¯ TP Mode", callback_data="set:room:tp_mode")],
        [InlineKeyboardButton("Ã°ÂŸÂ“Âˆ Trailing", callback_data="set:fg:trailing")],
        [InlineKeyboardButton("Ã°ÂŸÂ§Â· Break-even", callback_data="set:fg:breakeven")],
        [InlineKeyboardButton("Ã°ÂŸÂ’Â¼ Partials", callback_data="set:fg:partials")],
        [InlineKeyboardButton("Ã°ÂŸÂ”Â’ Profit Lock", callback_data="set:fg:profit_lock")],
        [InlineKeyboardButton("Ã¢Â†Â Settings", callback_data="settings_menu")],
    ])


def panel_settings_cat_capital_text(settings) -> str:
    return "\n".join([
        "Ã°ÂŸÂ“Â‰ *Settings Ã‚Â· Capital*",
        DIV,
        "Deployment posture and utilization targets.",
    ])


def kb_settings_cat_capital(settings) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Ã°ÂŸÂ’Â° Deployment", callback_data="set:fg:deployment")],
        [InlineKeyboardButton("Ã¢Â†Â Settings", callback_data="settings_menu")],
    ])


def panel_settings_cat_ai_text(settings) -> str:
    return "\n".join([
        "Ã°ÂŸÂ¤Â– *Settings Ã‚Â· AI*",
        DIV,
        "AI functionality has been removed from this build.",
    ])


def kb_settings_cat_ai(settings) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Ã°ÂŸÂ¤Â– AI Gate", callback_data="set:fg:ai_gate")],
        [InlineKeyboardButton("Ã¢Â†Â Settings", callback_data="settings_menu")],
    ])


def panel_settings_cat_diagnostics_text(settings) -> str:
    return "\n".join([
        "Ã°ÂŸÂ§Âª *Settings Ã‚Â· Diagnostics*",
        DIV,
        "Runtime visibility and diagnostic override controls.",
    ])


def kb_settings_cat_diagnostics(settings) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Ã°ÂŸÂ“Â¡ Telemetry", callback_data="set:room:v6_telemetry_enabled")],
        [InlineKeyboardButton("Ã°ÂŸÂ§Â¾ Block Detail", callback_data="set:room:block_diagnostics_enabled")],
        [InlineKeyboardButton("Ã°ÂŸÂ§Â¯ Debug Overrides", callback_data="diag:overrides")],
        [InlineKeyboardButton("Ã¢Â†Â Settings", callback_data="settings_menu")],
    ])


def panel_settings_cat_system_text(settings) -> str:
    return "\n".join([
        "Ã°ÂŸÂŽÂ›Ã¯Â¸Â *Settings Ã‚Â· System*",
        DIV,
        "Reset and housekeeping.",
        "",
        "_Session reset affects tracking only. Open trades are not touched._",
    ])


def kb_settings_cat_system(settings) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Ã°ÂŸÂ”Â„ Reset Session", callback_data="s:system_reset")],
        [InlineKeyboardButton("Ã¢Â†Âº Reset Defaults", callback_data="stg_reset_confirm")],
        [InlineKeyboardButton("Ã¢Â†Â Settings", callback_data="settings_menu")],
    ])


def _kb_room_nav(*, back_feature: str, back_category: str, back_settings: str = "settings_menu") -> list[list[InlineKeyboardButton]]:
    return [
        [InlineKeyboardButton("Ã¢Â†Â Feature", callback_data=back_feature)],
        [InlineKeyboardButton("Ã¢Â†Â Category", callback_data=back_category)],
        [InlineKeyboardButton("Ã¢Â†Â Settings", callback_data=back_settings)],
    ]


# Ã¢Â”Â€Ã¢Â”Â€ Feature Groups (Level 3) Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€

def panel_fg_slots_text(settings) -> str:
    return "\n".join([
        "Ã°ÂŸÂ“Â¦ *Slots & Allocation*",
        DIV,
        "Controls slot admission for NEW entries.",
    ])


def kb_fg_slots(settings) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Global Slot Mode", callback_data="set:room:global_slot_mode")],
        [InlineKeyboardButton("Max Positions", callback_data="set:room:max_open_positions")],
        [InlineKeyboardButton("Selection Policy", callback_data="set:room:trade_selection_policy")],
        [InlineKeyboardButton("Per-Engine Caps", callback_data="set:fg:engine_caps")],
        [InlineKeyboardButton("Ã¢Â†Â Trading", callback_data="set:cat:trading")],
        [InlineKeyboardButton("Ã¢Â†Â Settings", callback_data="settings_menu")],
    ])


def panel_fg_engine_caps_text(settings) -> str:
    return "\n".join([
        "Ã°ÂŸÂ§Â  *Per-Engine Caps*",
        DIV,
        "Per-engine slot caps apply to NEW entries only.",
    ])


def kb_fg_engine_caps(settings) -> InlineKeyboardMarkup:
    # Keep this compact; engines page is paged in the bot handler.
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("EnginesÃ¢Â€Â¦", callback_data="set:room:engine_caps:0")],
        [InlineKeyboardButton("Ã¢Â†Â Slots & Allocation", callback_data="set:fg:slots")],
        [InlineKeyboardButton("Ã¢Â†Â Trading", callback_data="set:cat:trading")],
        [InlineKeyboardButton("Ã¢Â†Â Settings", callback_data="settings_menu")],
    ])


def panel_fg_confidence_text(settings) -> str:
    return "\n".join([
        "Ã°ÂŸÂ§Â® *Confidence Thresholds*",
        DIV,
        "Affects NEW trade qualification only.",
    ])


def kb_fg_confidence(settings) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Min Vote Score", callback_data="set:room:min_vote_score")],
        [InlineKeyboardButton("V6 Micro Threshold", callback_data="set:room:v6_micro_confidence_threshold")],
        [InlineKeyboardButton("V6 Growth Threshold", callback_data="set:room:v6_growth_confidence_threshold")],
        [InlineKeyboardButton("V6 Pro Threshold", callback_data="set:room:v6_pro_confidence_threshold")],
        [InlineKeyboardButton("Ã¢Â†Â Strategy", callback_data="set:cat:strategy")],
        [InlineKeyboardButton("Ã¢Â†Â Settings", callback_data="settings_menu")],
    ])


def panel_fg_amaterasu_macro_text(settings) -> str:
    enabled = bool(settings.get_val("amaterasu_macro_layer_enabled", True))
    boost = float(settings.get_val("amaterasu_macro_conf_boost", 0.15) or 0.15)
    pen = float(settings.get_val("amaterasu_macro_conf_penalty", 0.10) or 0.10)
    szb = float(settings.get_val("amaterasu_macro_size_boost", 1.05) or 1.05)
    szp = float(settings.get_val("amaterasu_macro_size_penalty", 0.92) or 0.92)
    return "\n".join([
        "☀️ *AMATERASU Macro Layer*",
        DIV,
        f"State: `{'ON' if enabled else 'OFF'}`",
        f"Conf boost: `+{boost:.2f}`  ·  Conf penalty: `-{pen:.2f}`",
        f"Size boost: `x{szb:.2f}`  ·  Size penalty: `x{szp:.2f}`",
        "",
        "_Daily/weekly context weighting — not a hard filter._",
    ])


def kb_fg_amaterasu_macro(settings) -> InlineKeyboardMarkup:
    enabled = bool(settings.get_val("amaterasu_macro_layer_enabled", True))
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(f"{'🟢' if enabled else '🔴'} Enabled", callback_data="set:room:amaterasu_macro_layer_enabled")],
        [InlineKeyboardButton("Conf Boost", callback_data="set:room:amaterasu_macro_conf_boost"),
         InlineKeyboardButton("Conf Penalty", callback_data="set:room:amaterasu_macro_conf_penalty")],
        [InlineKeyboardButton("Size Boost", callback_data="set:room:amaterasu_macro_size_boost"),
         InlineKeyboardButton("Size Penalty", callback_data="set:room:amaterasu_macro_size_penalty")],
        [InlineKeyboardButton("← Strategy", callback_data="set:cat:strategy")],
        [InlineKeyboardButton("← Settings", callback_data="settings_menu")],
    ])


def panel_fg_weekly_cap_text(settings) -> str:
    mode = str(settings.get_val("weekly_cap_mode", "adaptive") or "adaptive").upper()
    enabled = bool(settings.get_val("weekly_cap_enabled", True))
    return "\n".join([
        "Ã°ÂŸÂ›Â¡Ã¯Â¸Â *Weekly Cap*",
        DIV,
        f"State: `{'ON' if enabled else 'OFF'}`  Ã‚Â·  Mode: `{mode}`",
        "",
        "_Admission controller for NEW entries only._",
    ])


def kb_fg_weekly_cap(settings) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Enable", callback_data="wcap:enable")],
        [InlineKeyboardButton("Mode", callback_data="wcap:mode")],
        [InlineKeyboardButton("Threshold", callback_data="wcap:threshold")],
        [InlineKeyboardButton("Recovery Threshold", callback_data="wcap:recovery")],
        [InlineKeyboardButton("Min Confidence", callback_data="wcap:minconf")],
        [InlineKeyboardButton("HQ-only", callback_data="wcap:hqonly")],
        [InlineKeyboardButton("Reduction", callback_data="wcap:reduction")],
        [InlineKeyboardButton("Cooldown", callback_data="wcap:cooldown")],
        [InlineKeyboardButton("Ã¢Â†Â Risk", callback_data="set:cat:risk")],
        [InlineKeyboardButton("Ã¢Â†Â Settings", callback_data="settings_menu")],
    ])


def panel_wcap_enable_text(settings) -> str:
    enabled = bool(settings.get_val("weekly_cap_enabled", True))
    return _room_header(
        "Ã°ÂŸÂ›Â¡Ã¯Â¸Â Weekly Cap Enable",
        "Turns weekly cap admission logic on/off for NEW entries.",
        "ON" if enabled else "OFF",
    )


def kb_wcap_enable(settings) -> InlineKeyboardMarkup:
    enabled = bool(settings.get_val("weekly_cap_enabled", True))
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("ON" + (" Ã¢Â—Â" if enabled else ""), callback_data="wcap:enable:on"),
            InlineKeyboardButton("OFF" + (" Ã¢Â—Â" if not enabled else ""), callback_data="wcap:enable:off"),
        ],
        *_kb_room_nav(back_feature="set:fg:weekly_cap", back_category="set:cat:risk"),
    ])


def panel_wcap_threshold_text(settings) -> str:
    thr = float(settings.get_val("weekly_cap_threshold_pct", 25.0) or 25.0)
    return _room_header(
        "Ã°ÂŸÂ›Â¡Ã¯Â¸Â Weekly Cap Threshold",
        "At/above this weekly drawdown %, weaker trades are reduced/blocked.",
        f"{thr:.1f}%",
    )


def kb_wcap_threshold(settings) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Ã¢ÂˆÂ’", callback_data="wcap:threshold:dn"),
         InlineKeyboardButton("+", callback_data="wcap:threshold:up")],
        *_kb_room_nav(back_feature="set:fg:weekly_cap", back_category="set:cat:risk"),
    ])


def panel_wcap_recovery_text(settings) -> str:
    rec = float(settings.get_val("weekly_cap_recovery_threshold_pct", 15.0) or 15.0)
    return _room_header(
        "Ã°ÂŸÂ›Â¡Ã¯Â¸Â Recovery Threshold",
        "Soft band: start reducing/guarding above this drawdown %.",
        f"{rec:.1f}%",
    )


def kb_wcap_recovery(settings) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Ã¢ÂˆÂ’", callback_data="wcap:recovery:dn"),
         InlineKeyboardButton("+", callback_data="wcap:recovery:up")],
        *_kb_room_nav(back_feature="set:fg:weekly_cap", back_category="set:cat:risk"),
    ])


def panel_wcap_minconf_text(settings) -> str:
    mc = float(settings.get_val("weekly_cap_min_confidence", 4.2) or 4.2)
    return _room_header(
        "Ã°ÂŸÂ›Â¡Ã¯Â¸Â Min Confidence",
        "Minimum confidence required to pass when weekly cap posture is active.",
        f"{mc:.2f}",
    )


def kb_wcap_minconf(settings) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Ã¢ÂˆÂ’", callback_data="wcap:minconf:dn"),
         InlineKeyboardButton("+", callback_data="wcap:minconf:up")],
        *_kb_room_nav(back_feature="set:fg:weekly_cap", back_category="set:cat:risk"),
    ])


def panel_wcap_hqonly_text(settings) -> str:
    hq = bool(settings.get_val("weekly_cap_allow_high_conf_only", True))
    return _room_header(
        "Ã°ÂŸÂ›Â¡Ã¯Â¸Â HQ-only",
        "Above threshold: allow only clearly strong setups (reduced).",
        "ON" if hq else "OFF",
    )


def kb_wcap_hqonly(settings) -> InlineKeyboardMarkup:
    hq = bool(settings.get_val("weekly_cap_allow_high_conf_only", True))
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("ON" + (" Ã¢Â—Â" if hq else ""), callback_data="wcap:hqonly:on"),
            InlineKeyboardButton("OFF" + (" Ã¢Â—Â" if not hq else ""), callback_data="wcap:hqonly:off"),
        ],
        *_kb_room_nav(back_feature="set:fg:weekly_cap", back_category="set:cat:risk"),
    ])


def panel_wcap_reduction_text(settings) -> str:
    red = float(settings.get_val("weekly_cap_size_reduction", 0.5) or 0.5)
    return _room_header(
        "Ã°ÂŸÂ›Â¡Ã¯Â¸Â Size Reduction",
        "Multiplier applied to size when a trade is ACCEPT_REDUCED.",
        f"x{red:.2f}",
    )


def kb_wcap_reduction(settings) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Ã¢ÂˆÂ’", callback_data="wcap:reduction:dn"),
         InlineKeyboardButton("+", callback_data="wcap:reduction:up")],
        *_kb_room_nav(back_feature="set:fg:weekly_cap", back_category="set:cat:risk"),
    ])


def panel_wcap_cooldown_text(settings) -> str:
    cd = int(float(settings.get_val("weekly_cap_cooldown_seconds", 120) or 120))
    return _room_header(
        "Ã°ÂŸÂ›Â¡Ã¯Â¸Â Cooldown (sec)",
        "Cooldown after reduced accepts (high-quality trades can still pass reduced).",
        str(cd),
    )


def kb_wcap_cooldown(settings) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Ã¢ÂˆÂ’", callback_data="wcap:cooldown:dn"),
         InlineKeyboardButton("+", callback_data="wcap:cooldown:up")],
        *_kb_room_nav(back_feature="set:fg:weekly_cap", back_category="set:cat:risk"),
    ])


def panel_fg_trailing_text(settings) -> str:
    on = bool(settings.get_val("tsl_enabled", True))
    return "\n".join([
        "Ã°ÂŸÂ“Âˆ *Trailing*",
        DIV,
        f"Current: `{'ON' if on else 'OFF'}`",
        "",
        "_Applies to exit management; safe for existing trades._",
    ])


def kb_fg_trailing(settings) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Enabled", callback_data="set:room:tsl_enabled")],
        [InlineKeyboardButton("Activation %", callback_data="set:room:trailing_activation_pct")],
        [InlineKeyboardButton("Trail %", callback_data="set:room:trailing_distance_pct")],
        [InlineKeyboardButton("After TP1 %", callback_data="set:room:trailing_after_tp1_pct")],
        [InlineKeyboardButton("After TP2 %", callback_data="set:room:trailing_after_tp2_pct")],
        [InlineKeyboardButton("Ã¢Â†Â Execution", callback_data="set:cat:execution")],
        [InlineKeyboardButton("Ã¢Â†Â Settings", callback_data="settings_menu")],
    ])


def panel_fg_breakeven_text(settings) -> str:
    on = bool(settings.get_val("breakeven_enabled", True))
    return "\n".join([
        "Ã°ÂŸÂ§Â· *Break-even*",
        DIV,
        f"Current: `{'ON' if on else 'OFF'}`",
        "",
        "_Applies to exit management; safe for existing trades._",
    ])


def kb_fg_breakeven(settings) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Enabled", callback_data="set:room:breakeven_enabled")],
        [InlineKeyboardButton("Trigger %", callback_data="set:room:breakeven_trigger_pct")],
        [InlineKeyboardButton("Activation ($)", callback_data="set:room:breakeven_activation_profit_usd")],
        [InlineKeyboardButton("Buffer %", callback_data="set:room:breakeven_buffer_pct")],
        [InlineKeyboardButton("Ã¢Â†Â Execution", callback_data="set:cat:execution")],
        [InlineKeyboardButton("Ã¢Â†Â Settings", callback_data="settings_menu")],
    ])


def panel_fg_partials_text(settings) -> str:
    on = bool(settings.get_val("partial_exit_enabled", True))
    return "\n".join([
        "Ã°ÂŸÂ’Â¼ *Partials*",
        DIV,
        f"Current: `{'ON' if on else 'OFF'}`",
        "",
        "_Applies to exit management; safe for existing trades._",
    ])


def kb_fg_partials(settings) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Enabled", callback_data="set:room:partial_exit_enabled")],
        [InlineKeyboardButton("TP1 Trigger %", callback_data="set:room:tp1_trigger_pct")],
        [InlineKeyboardButton("TP1 Close %", callback_data="set:room:partial_tp1_close_pct")],
        [InlineKeyboardButton("TP2 Trigger %", callback_data="set:room:tp2_trigger_pct")],
        [InlineKeyboardButton("TP2 Close %", callback_data="set:room:partial_tp2_close_pct")],
        [InlineKeyboardButton("Min Net TP ($)", callback_data="set:room:min_net_take_profit_usdt")],
        [InlineKeyboardButton("Min Full Close ($)", callback_data="set:room:min_full_close_profit_usdt")],
        [InlineKeyboardButton("Ã¢Â†Â Execution", callback_data="set:cat:execution")],
        [InlineKeyboardButton("Ã¢Â†Â Settings", callback_data="settings_menu")],
    ])


def panel_fg_profit_lock_text(settings) -> str:
    on = bool(settings.get_val("profit_lock_enabled", True))
    return "\n".join([
        "Ã°ÂŸÂ”Â’ *Profit Lock*",
        DIV,
        f"Current: `{'ON' if on else 'OFF'}`",
        "",
        "_Applies to exit management; safe for existing trades._",
    ])


def kb_fg_profit_lock(settings) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Enabled", callback_data="set:room:profit_lock_enabled")],
        [InlineKeyboardButton("Trigger %", callback_data="set:room:profit_lock_trigger_pct")],
        [InlineKeyboardButton("Buffer %", callback_data="set:room:profit_lock_buffer_pct")],
        [InlineKeyboardButton("Minimum Net Capture", callback_data="set:room:minimum_net_capture_fee_multiple")],
        [InlineKeyboardButton("Min Full Close ($)", callback_data="set:room:min_full_close_profit_usdt")],
        [InlineKeyboardButton("Ã¢Â†Â Execution", callback_data="set:cat:execution")],
        [InlineKeyboardButton("Ã¢Â†Â Settings", callback_data="settings_menu")],
    ])


# Ã¢Â”Â€Ã¢Â”Â€ Rooms (Level 4) Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€Ã¢Â”Â€

def _room_header(title: str, desc: str, current: str) -> str:
    return "\n".join([title, DIV, desc, "", f"Current: `{current}`"])


def panel_room_global_slot_mode(settings) -> str:
    cur = str(settings.get_val("global_slot_mode", "fixed") or "fixed").lower()
    cur = cur if cur in {"fixed", "auto"} else "fixed"
    return _room_header(
        "Ã°ÂŸÂ“Â¦ Global Slot Mode",
        "Controls how total slot availability is computed for NEW entries.",
        cur.upper(),
    )


def kb_room_global_slot_mode(settings) -> InlineKeyboardMarkup:
    cur = str(settings.get_val("global_slot_mode", "fixed") or "fixed").lower()
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("FIXED" + (" Ã¢Â—Â" if cur == "fixed" else ""), callback_data="room:global_slot_mode:fixed"),
            InlineKeyboardButton("AUTO" + (" Ã¢Â—Â" if cur == "auto" else ""), callback_data="room:global_slot_mode:auto"),
        ],
        *_kb_room_nav(back_feature="set:fg:slots", back_category="set:cat:trading"),
    ])


def panel_room_max_open_positions(settings) -> str:
    cur = int(settings.get_val("max_open_positions", 10) or 10)
    return _room_header(
        "Ã°ÂŸÂ“ÂŠ Max Positions",
        "Fixed-mode hard cap for NEW positions.",
        str(cur),
    )


def kb_room_max_open_positions(settings) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Ã¢ÂˆÂ’", callback_data="room:max_open_positions:dn"),
         InlineKeyboardButton("+", callback_data="room:max_open_positions:up")],
        *_kb_room_nav(back_feature="set:fg:slots", back_category="set:cat:trading"),
    ])


def panel_room_trade_selection_policy(settings) -> str:
    cur = str(settings.get_val("trade_selection_policy", "best_score") or "best_score").lower()
    cur = cur if cur in {"first_come", "best_score", "replace_weakest"} else "best_score"
    label = {"first_come": "FIRST COME", "best_score": "BEST SCORE", "replace_weakest": "REPLACE WEAKEST"}[cur]
    return _room_header(
        "Ã°ÂŸÂŽÂ¯ Selection Policy",
        "Controls ordering and replacement for NEW entries.",
        label,
    )


def kb_room_trade_selection_policy(settings) -> InlineKeyboardMarkup:
    cur = str(settings.get_val("trade_selection_policy", "best_score") or "best_score").lower()
    def _btn(lbl: str, val: str) -> InlineKeyboardButton:
        return InlineKeyboardButton(lbl + (" Ã¢Â—Â" if cur == val else ""), callback_data=f"room:trade_selection_policy:{val}")
    return InlineKeyboardMarkup([
        [_btn("FIRST", "first_come"), _btn("BEST", "best_score"), _btn("REPLACE", "replace_weakest")],
        *_kb_room_nav(back_feature="set:fg:slots", back_category="set:cat:strategy"),
    ])


def panel_room_engine_caps_list_text(settings, page: int = 0) -> str:
    page = max(int(page or 0), 0)
    return "\n".join([
        "Ã°ÂŸÂ§Â  *Per-Engine Caps*",
        DIV,
        "Pick an engine to configure its cap for NEW entries.",
        "",
        f"Page: `{page + 1}`",
    ])


def kb_room_engine_caps_list(settings, page: int = 0, page_size: int = 9) -> InlineKeyboardMarkup:
    try:
        from category_tracker import ENGINE_NAMES as _ENG
        names = [str(n).upper() for n in (_ENG or [])]
    except Exception:
        names = []
    names = names or ["HACHIMAN"]
    page = max(int(page or 0), 0)
    page_size = max(int(page_size or 9), 3)
    start = page * page_size
    subset = names[start:start + page_size]

    rows: list[list[InlineKeyboardButton]] = []
    for i in range(0, len(subset), 3):
        chunk = subset[i:i + 3]
        rows.append([
            InlineKeyboardButton(n, callback_data=f"room:engine_cap:{n}:{page}")
            for n in chunk
        ])

    nav_row: list[InlineKeyboardButton] = []
    if start > 0:
        nav_row.append(InlineKeyboardButton("Ã¢Â†Â Prev", callback_data=f"set:room:engine_caps:{page - 1}"))
    if start + page_size < len(names):
        nav_row.append(InlineKeyboardButton("Next Ã¢Â†Â’", callback_data=f"set:room:engine_caps:{page + 1}"))
    if nav_row:
        rows.append(nav_row)

    rows += _kb_room_nav(back_feature="set:fg:engine_caps", back_category="set:cat:trading")
    return InlineKeyboardMarkup(rows)


def _engine_cap_snapshot(settings, eng: str) -> tuple[str, int]:
    e = str(eng or "").strip().lower()
    if not e:
        return "inherit", 0
    mode = str(settings.get_val(f"engine_{e}_slot_mode", "inherit") or "inherit").lower()
    mode = mode if mode in {"fixed", "inherit", "unlimited"} else "inherit"
    cap = int(settings.get_val(f"engine_{e}_max_pos", 0) or 0)
    return mode, cap


def panel_room_engine_cap_text(settings, eng: str) -> str:
    e = str(eng or "").strip().upper() or "?"
    mode, cap = _engine_cap_snapshot(settings, e)
    cap_s = "INHERIT" if mode == "inherit" or cap == 0 else ("UNLIMITED" if cap < 0 or mode == "unlimited" else str(cap))
    return _room_header(
        f"Ã°ÂŸÂ§Â  Engine Cap Ã‚Â· {e}",
        "Controls how many NEW positions this engine can open.",
        f"{mode.upper()} Ã‚Â· {cap_s}",
    )


def kb_room_engine_cap(settings, eng: str, back_page: int = 0) -> InlineKeyboardMarkup:
    e = str(eng or "").strip().upper() or "HACHIMAN"
    mode, cap = _engine_cap_snapshot(settings, e)

    def _active(label: str, is_on: bool) -> str:
        return label + (" Ã¢Â—Â" if is_on else "")

    inherit_on = (mode == "inherit") or (cap == 0)
    unl_on = (mode == "unlimited") or (cap < 0)
    fixed_cap = cap if cap > 0 else 1

    return InlineKeyboardMarkup([
        [InlineKeyboardButton(_active("INHERIT", inherit_on), callback_data=f"room:engine_cap_set:{e}:inherit:{back_page}")],
        [
            InlineKeyboardButton("Ã¢ÂˆÂ’", callback_data=f"room:engine_cap_adj:{e}:dn:{back_page}"),
            InlineKeyboardButton(_active(f"FIXED {fixed_cap}", (mode == "fixed" and not inherit_on and not unl_on)), callback_data=f"room:engine_cap:{e}:{back_page}"),
            InlineKeyboardButton("+", callback_data=f"room:engine_cap_adj:{e}:up:{back_page}"),
        ],
        [InlineKeyboardButton(_active("UNLIMITED", unl_on), callback_data=f"room:engine_cap_set:{e}:unlimited:{back_page}")],
        [InlineKeyboardButton("Ã¢Â†Â Engines", callback_data=f"set:room:engine_caps:{back_page}")],
        *_kb_room_nav(back_feature="set:fg:engine_caps", back_category="set:cat:trading"),
    ])


def panel_room_tp_mode(settings) -> str:
    cur = str(settings.get_val("tp_mode", "hybrid") or "hybrid").lower()
    cur = cur if cur in {"off", "reference_only", "exchange_tp", "hybrid"} else "hybrid"
    owner = derive_tp_owner(
        cur,
        partial_enabled=bool(settings.get_val("partial_exit_enabled", True)),
    )
    return _room_header(
        "Ã°ÂŸÂŽÂ¯ TP Mode",
        "Controls how Take Profit is attached and displayed.",
        f"{cur.upper()} | OWNER {owner}",
    )


def kb_room_tp_mode(settings) -> InlineKeyboardMarkup:
    cur = str(settings.get_val("tp_mode", "hybrid") or "hybrid").lower()
    def _btn(lbl: str, val: str) -> InlineKeyboardButton:
        return InlineKeyboardButton(lbl + (" Ã¢Â—Â" if cur == val else ""), callback_data=f"room:tp_mode:{val}")
    return InlineKeyboardMarkup([
        [_btn("OFF", "off"), _btn("REF", "reference_only")],
        [_btn("EXCH", "exchange_tp"), _btn("HYBRID", "hybrid")],
        *_kb_room_nav(back_feature="set:cat:execution", back_category="set:cat:execution"),
    ])


def panel_room_bool(settings, key: str, title: str, desc: str) -> str:
    cur = bool(settings.get_val(key, False))
    return _room_header(title, desc, "ON" if cur else "OFF")


def kb_room_bool(*, on: bool, cb_on: str, cb_off: str, back_feature: str, back_category: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("ON" + (" Ã¢Â—Â" if on else ""), callback_data=cb_on),
            InlineKeyboardButton("OFF" + (" Ã¢Â—Â" if not on else ""), callback_data=cb_off),
        ],
        *_kb_room_nav(back_feature=back_feature, back_category=back_category),
    ])


def panel_room_number(settings, key: str, title: str, desc: str, fmt: str = "{v}") -> str:
    v = settings.get_val(key, 0)
    try:
        vv = float(v)
        cur = fmt.format(v=vv)
    except Exception:
        cur = str(v)
    return _room_header(title, desc, cur)


def kb_room_number(*, cb_dn: str, cb_up: str, back_feature: str, back_category: str, cb_custom: str | None = None) -> InlineKeyboardMarkup:
    controls = [InlineKeyboardButton("-", callback_data=cb_dn), InlineKeyboardButton("+", callback_data=cb_up)]
    if cb_custom:
        controls.append(InlineKeyboardButton("Custom", callback_data=cb_custom))
    return InlineKeyboardMarkup([
        controls,
        *_kb_room_nav(back_feature=back_feature, back_category=back_category),
    ])


def panel_fg_deployment_text(settings) -> str:
    mode = str(settings.get_val("deployment_mode", "Guarded") or "Guarded")
    return "\n".join([
        "Ã°ÂŸÂ’Â° *Deployment*",
        DIV,
        f"Mode: `{mode}`",
        "",
        "Affects NEW sizing/allocation decisions.",
        "Real controls: base size, floors/caps, balance cap, dynamic sizing, and deployment budget.",
    ])


def kb_fg_deployment(settings) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Base Trade Size", callback_data="set:room:position_size_usdt")],
        [InlineKeyboardButton("Min Size", callback_data="set:room:v6_min_position_size_usdt")],
        [InlineKeyboardButton("Max Size", callback_data="set:room:v6_max_position_size_usdt")],
        [InlineKeyboardButton("Balance % Cap", callback_data="set:room:v6_max_position_pct_of_balance")],
        [InlineKeyboardButton("Dynamic Sizing", callback_data="set:room:dynamic_sizing_enabled")],
        [InlineKeyboardButton("Balance-Based Size %", callback_data="set:room:position_size_pct")],
        [InlineKeyboardButton("Deployment Mode", callback_data="set:room:deployment_mode")],
        [InlineKeyboardButton("Utilization Target", callback_data="set:room:deployment_target_utilization_pct")],
        [InlineKeyboardButton("Deploy Max Positions", callback_data="set:room:deployment_max_positions")],
        [InlineKeyboardButton("Ã¢Â†Â Capital", callback_data="set:cat:capital")],
        [InlineKeyboardButton("Ã¢Â†Â Settings", callback_data="settings_menu")],
    ])


def panel_fg_ai_gate_text(settings) -> str:
    on = bool(settings.get_val("v6_ai_veto_enabled", False))
    fallback = str(settings.get_val("v6_ai_gate_fallback", "bypass") or "bypass").upper()
    return "\n".join([
        "Ã°ÂŸÂ¤Â– *AI Gate*",
        DIV,
        f"Enabled: `{'ON' if on else 'OFF'}`  Ã‚Â·  Fallback: `{fallback}`",
        "",
        "_If AI fails, trading continues with the configured fallback._",
    ])


def kb_fg_ai_gate(settings) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("AI Removed", callback_data="diag:control_state")],
        [InlineKeyboardButton("Ã¢Â†Â AI", callback_data="set:cat:ai")],
        [InlineKeyboardButton("Ã¢Â†Â Settings", callback_data="settings_menu")],
    ])


def panel_fg_exposure_text(settings) -> str:
    mode = str(settings.get_val("exposure_control_mode", "Strict") or "Strict")
    return "\n".join([
        "Ã°ÂŸÂ“Â‰ *Exposure*",
        DIV,
        f"Mode: `{mode}`",
        "",
        "Affects NEW admission decisions only.",
    ])


def kb_fg_exposure(settings) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Exposure Mode", callback_data="set:room:exposure_control_mode")],
        [InlineKeyboardButton("Ã¢Â†Â Risk", callback_data="set:cat:risk")],
        [InlineKeyboardButton("Ã¢Â†Â Settings", callback_data="settings_menu")],
    ])


def panel_fg_risk_ceiling_text(settings) -> str:
    enabled = bool(settings.get_val("realized_risk_ceiling_enabled", True))
    pct = float(settings.get_val("realized_risk_cap_pct_balance", 1.0) or 1.0)
    mn = float(settings.get_val("realized_risk_cap_min_usdt", 1.0) or 1.0)
    mx = float(settings.get_val("realized_risk_cap_max_usdt", 3.0) or 3.0)
    tighten = bool(settings.get_val("realized_risk_ceiling_tighten_sl_if_needed", True))
    telem = bool(settings.get_val("realized_risk_ceiling_telemetry_enabled", True))
    return "\n".join([
        "Ã°ÂŸÂ§Â¯ *Realized Risk Ceiling*",
        DIV,
        f"State: `{'ON' if enabled else 'OFF'}`",
        f"Cap: `{pct:.2f}%` of balance  (min `{mn:.2f}` / max `{mx:.2f}` USDT)",
        f"Tighten SL if needed: `{'ON' if tighten else 'OFF'}`",
        f"Telemetry: `{'ON' if telem else 'OFF'}`",
        "",
        "_Non-blocking: prefers resizing; tightens SL only when required to stay executable._",
    ])


def kb_fg_risk_ceiling(settings) -> InlineKeyboardMarkup:
    enabled = bool(settings.get_val("realized_risk_ceiling_enabled", True))
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(f"{'🟢' if enabled else '🔴'} Enabled", callback_data="set:room:realized_risk_ceiling_enabled")],
        [InlineKeyboardButton("Cap %", callback_data="set:room:realized_risk_cap_pct_balance"),
         InlineKeyboardButton("Min USDT", callback_data="set:room:realized_risk_cap_min_usdt")],
        [InlineKeyboardButton("Max USDT", callback_data="set:room:realized_risk_cap_max_usdt")],
        [InlineKeyboardButton("Tighten SL", callback_data="set:room:realized_risk_ceiling_tighten_sl_if_needed"),
         InlineKeyboardButton("Telemetry", callback_data="set:room:realized_risk_ceiling_telemetry_enabled")],
        [InlineKeyboardButton("← Risk", callback_data="set:cat:risk")],
        [InlineKeyboardButton("← Settings", callback_data="settings_menu")],
    ])


def panel_fg_short_rehab_text(settings) -> str:
    enabled = bool(settings.get_val("short_rehab_enabled", True))
    mult = float(settings.get_val("short_rehab_weak_regime_size_mult", 0.90) or 0.90)
    adx = float(settings.get_val("short_rehab_min_adx_1h", 18.0) or 18.0)
    return "\n".join([
        "🩹 *Short Rehab*",
        DIV,
        f"State: `{'ON' if enabled else 'OFF'}`",
        f"Weak/chop size×: `x{mult:.2f}`",
        f"1H ADX floor: `{adx:.1f}`",
        "",
        "_Non-blocking: size-only adjustment for shorts in weak conditions._",
    ])


def kb_fg_short_rehab(settings) -> InlineKeyboardMarkup:
    enabled = bool(settings.get_val("short_rehab_enabled", True))
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(f"{'🟢' if enabled else '🔴'} Enabled", callback_data="set:room:short_rehab_enabled")],
        [InlineKeyboardButton("Size×", callback_data="set:room:short_rehab_weak_regime_size_mult"),
         InlineKeyboardButton("ADX Floor", callback_data="set:room:short_rehab_min_adx_1h")],
        [InlineKeyboardButton("← Risk", callback_data="set:cat:risk")],
        [InlineKeyboardButton("← Settings", callback_data="settings_menu")],
    ])



def panel_weekly_cap_hub_text(settings, balance: float | None = None) -> str:
    bal = max(float(balance or 0.0), 0.0)
    try:
        import weekly_cap as _w
        dd = _w.get_weekly_drawdown_pct(bal) if bal > 0 else 0.0
    except Exception:
        dd = 0.0

    mode = str(settings.get_val("weekly_cap_mode", "adaptive") or "adaptive").upper()
    enabled = bool(settings.get_val("weekly_cap_enabled", True))
    thr = float(settings.get_val("weekly_cap_threshold_pct", 25.0) or 25.0)
    rec = float(settings.get_val("weekly_cap_recovery_threshold_pct", 15.0) or 15.0)
    min_conf = float(settings.get_val("weekly_cap_min_confidence", 4.2) or 4.2)
    hq_only = bool(settings.get_val("weekly_cap_allow_high_conf_only", True))
    red = float(settings.get_val("weekly_cap_size_reduction", 0.5) or 0.5)
    cd = int(float(settings.get_val("weekly_cap_cooldown_seconds", 120) or 120))

    return "\n".join([
        "Ã°ÂŸÂ›Â¡Ã¯Â¸Â *Weekly Cap*",
        DIV,
        f"State: `{'ON' if enabled else 'OFF'}`  Ã‚Â·  Mode: `{mode}`",
        f"Weekly DD: `{dd:.1f}%`  Ã‚Â·  Thr `{thr:.1f}%`  Ã‚Â·  Recovery `{rec:.1f}%`",
        f"Min conf: `{min_conf:.2f}`  Ã‚Â·  HQ-only: `{'YES' if hq_only else 'NO'}`",
        f"Reduced size: `x{red:.2f}`  Ã‚Â·  Cooldown: `{cd}s`",
        "",
        "_Admission controller for NEW entries only._",
    ])


def kb_weekly_cap_hub(settings) -> InlineKeyboardMarkup:
    enabled = bool(settings.get_val("weekly_cap_enabled", True))
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(f"{'Ã°ÂŸÂŸÂ¢' if enabled else 'Ã°ÂŸÂ”Â´'} Enabled", callback_data="wcap:enabled:toggle")],
        [InlineKeyboardButton("Mode", callback_data="wcap:mode"),
         InlineKeyboardButton("Thresholds", callback_data="wcap:thresholds")],
        [InlineKeyboardButton("Quality Rules", callback_data="wcap:quality"),
         InlineKeyboardButton("Reduction", callback_data="wcap:reduction")],
        [InlineKeyboardButton("Ã¢Â†Â Risk", callback_data="set:cat:risk")],
    ])


def panel_weekly_cap_mode_text(settings) -> str:
    mode = str(settings.get_val("weekly_cap_mode", "adaptive") or "adaptive").lower()
    mode = mode if mode in {"off", "soft", "strict", "adaptive"} else "adaptive"
    return "\n".join([
        "Ã°ÂŸÂ›Â¡Ã¯Â¸Â *Weekly Cap Ã‚Â· Mode*",
        DIV,
        f"Current: `{mode.upper()}`",
        "",
        "Controls how drawdown protection shapes NEW entries.",
    ])


def kb_weekly_cap_mode(settings) -> InlineKeyboardMarkup:
    cur = str(settings.get_val("weekly_cap_mode", "adaptive") or "adaptive").lower()

    def _dot(label: str, active: bool) -> str:
        return f"{'Ã¢Â—Â ' if active else ''}{label}"

    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton(_dot("OFF", cur == "off"), callback_data="wcap:mode:off"),
            InlineKeyboardButton(_dot("SOFT", cur == "soft"), callback_data="wcap:mode:soft"),
        ],
        [
            InlineKeyboardButton(_dot("STRICT", cur == "strict"), callback_data="wcap:mode:strict"),
            InlineKeyboardButton(_dot("ADAPTIVE", cur == "adaptive"), callback_data="wcap:mode:adaptive"),
        ],
        [InlineKeyboardButton("Ã¢Â†Â Weekly Cap", callback_data="wcap:hub")],
    ])


def panel_weekly_cap_thresholds_text(settings) -> str:
    thr = float(settings.get_val("weekly_cap_threshold_pct", 25.0) or 25.0)
    rec = float(settings.get_val("weekly_cap_recovery_threshold_pct", 15.0) or 15.0)
    return "\n".join([
        "Ã°ÂŸÂ›Â¡Ã¯Â¸Â *Weekly Cap Ã‚Â· Thresholds*",
        DIV,
        f"Threshold: `{thr:.1f}%`",
        f"Recovery:  `{rec:.1f}%`",
        "",
        "_Recovery is the soft band. Above threshold, only stronger setups pass (often reduced)._",
    ])


def kb_weekly_cap_thresholds(settings) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Thr Ã¢ÂˆÂ’", callback_data="wcap:thr:dn"),
         InlineKeyboardButton("Thr +", callback_data="wcap:thr:up")],
        [InlineKeyboardButton("Rec Ã¢ÂˆÂ’", callback_data="wcap:rec:dn"),
         InlineKeyboardButton("Rec +", callback_data="wcap:rec:up")],
        [InlineKeyboardButton("Ã¢Â†Â Weekly Cap", callback_data="wcap:hub")],
    ])


def panel_weekly_cap_quality_text(settings) -> str:
    min_conf = float(settings.get_val("weekly_cap_min_confidence", 4.2) or 4.2)
    hq_only = bool(settings.get_val("weekly_cap_allow_high_conf_only", True))
    return "\n".join([
        "Ã°ÂŸÂ›Â¡Ã¯Â¸Â *Weekly Cap Ã‚Â· Quality Rules*",
        DIV,
        f"Min confidence: `{min_conf:.2f}`",
        f"HQ-only above threshold: `{'YES' if hq_only else 'NO'}`",
        "",
        "_HQ-only means: allow only clearly strong setups when DD is high._",
    ])


def kb_weekly_cap_quality(settings) -> InlineKeyboardMarkup:
    hq_only = bool(settings.get_val("weekly_cap_allow_high_conf_only", True))
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Conf Ã¢ÂˆÂ’", callback_data="wcap:minconf:dn"),
         InlineKeyboardButton("Conf +", callback_data="wcap:minconf:up")],
        [InlineKeyboardButton(f"HQ-only: {'ON' if hq_only else 'OFF'}", callback_data="wcap:hqonly:toggle")],
        [InlineKeyboardButton("Ã¢Â†Â Weekly Cap", callback_data="wcap:hub")],
    ])


def panel_weekly_cap_reduction_text(settings) -> str:
    red = float(settings.get_val("weekly_cap_size_reduction", 0.5) or 0.5)
    cd = int(float(settings.get_val("weekly_cap_cooldown_seconds", 120) or 120))
    return "\n".join([
        "Ã°ÂŸÂ›Â¡Ã¯Â¸Â *Weekly Cap Ã‚Â· Reduction*",
        DIV,
        f"Reduced size multiplier: `x{red:.2f}`",
        f"Cooldown (seconds): `{cd}`",
        "",
        "_Cooldown applies after ACCEPT_REDUCED. High-quality trades can still pass reduced._",
    ])


def kb_weekly_cap_reduction(settings) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Size Ã¢ÂˆÂ’", callback_data="wcap:red:dn"),
         InlineKeyboardButton("Size +", callback_data="wcap:red:up")],
        [InlineKeyboardButton("CD Ã¢ÂˆÂ’", callback_data="wcap:cd:dn"),
         InlineKeyboardButton("CD +", callback_data="wcap:cd:up")],
        [InlineKeyboardButton("Ã¢Â†Â Weekly Cap", callback_data="wcap:hub")],
    ])


def panel_settings_system_text(settings) -> str:
    return "\n".join([
        "Ã°ÂŸÂ§Â° *System*",
        DIV,
        "Session reset clears internal tracking/diagnostics so you can start fresh.",
        "",
        "_Open trades, OKX orders, execution, sync, and exit plans are NOT touched._",
        HOT_SCOPE_NOTE,
    ])


def kb_settings_system(settings) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Ã°ÂŸÂ”Â„ Reset", callback_data="s:system_reset")],
        [InlineKeyboardButton("Ã°ÂŸÂ”Â™ Settings", callback_data="settings_menu")],
    ])


def panel_settings_system_reset_text(settings) -> str:
    last_reset = "Ã¢Â€Â”"
    uptime = "Ã¢Â€Â”"
    try:
        import reset_manager as _rm

        ts = str(_rm.get_last_reset_timestamp() or "").strip()
        if ts:
            last_reset = ts.replace("T", " ").replace("+00:00", " UTC")[:19] + " UTC"
        uptime = _rm.format_uptime(_rm.get_session_uptime_seconds())
    except Exception:
        pass

    return "\n".join([
        "Ã°ÂŸÂ”Â„ *Reset Session*",
        DIV,
        f"Last reset: `{last_reset}`",
        f"Session uptime: `{uptime}`",
        "",
        "Resets:",
        "Ã¢Â€Â¢ performance stats (weekly pnl, winrate counters, drawdown tracking)",
        "Ã¢Â€Â¢ weekly-cap cooldown memory",
        "Ã¢Â€Â¢ winrate learning state (engine weights)",
        "Ã¢Â€Â¢ diagnostics buffers (blocked/probe history)",
        "Ã¢Â€Â¢ AI degraded/cooldown flags",
        "",
        "_Open trades will NOT be affected._",
    ])


def kb_settings_system_reset(settings) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Ã°ÂŸÂ”Â„ Reset Session", callback_data="sys:reset_start")],
        [InlineKeyboardButton("Ã¢ÂÂŒ Cancel", callback_data="s:system")],
    ])


def panel_settings_system_reset_confirm_text() -> str:
    return "\n".join([
        "Ã¢ÂšÂ Ã¯Â¸Â *Confirm Session Reset*",
        DIV,
        "This will reset all performance stats and risk tracking.",
        "Open trades will NOT be affected.",
        "",
        "Continue?",
    ])


def kb_system_reset_confirm() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Ã¢ÂšÂ Ã¯Â¸Â Confirm Reset", callback_data="sys:reset_confirm"),
         InlineKeyboardButton("Ã¢ÂÂŒ Cancel", callback_data="sys:reset_cancel")],
    ])


def kb_system_reset_done() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Ã°ÂŸÂ”Â™ System", callback_data="s:system"),
         InlineKeyboardButton("Ã°ÂŸÂ§Âª Diagnostics", callback_data="s:diagnostics")],
    ])


def panel_settings_dashboard_text(
    settings,


    *,


    balance: float | None = None,


    exposure=None,


    running: bool = False,


    status_label: str = "STOPPED",


    regime: str = "Ã¢Â€Â”",


    session: str = "Ã¢Â€Â”",


    sync_status: str = "Standby",


    scanner_status: str = "Idle",


    open_positions: int = 0,


) -> str:


    snap = _strategy_runtime_snapshot(settings, balance=balance)


    status_icon = "Ã°ÂŸÂŸÂ¢" if status_label in {"LIVE", "LIVE READY"} else ("Ã°ÂŸÂŸÂ¡" if "PAPER" in status_label else "Ã°ÂŸÂ”Â´")


    exposure_cap = float(snap.get("exposure_cap", 0.0) or 0.0)


    exposure_line = "n/a"


    if exposure is not None and snap["balance"] > 0 and exposure_cap > 0:


        current = float(getattr(exposure, "total_usdt", 0.0) or 0.0)


        current_pct = current / snap["balance"] * 100.0 if snap["balance"] else 0.0


        cap_pct = exposure_cap / snap["balance"] * 100.0 if snap["balance"] else 0.0


        exposure_line = f"`{current_pct:.1f}%` / `{cap_pct:.1f}%`  (`${current:.2f}` / `${exposure_cap:.2f}`)"





    protection = (


        f"DD `{_state_text(settings, 'v6_anti_drawdown_enabled', True)}`"


        f" Ã‚Â· Open-Risk `{float(settings.get_val('rule_357_max_open_risk_pct', 25.0)):.1f}%`"


        " Ã‚Â· Exit `ACTIVE`"


    )


    execution = (


        f"`{snap['exec_mode']}`"


        f" Ã‚Â· Fallback `{_state_text(settings, 'leverage_fallback_enabled', True)}`"


        f" Ã‚Â· Cooldown `{_state_text(settings, 'preflight_cooldown_enabled', True)}`"


    )

    tp_attach = str(settings.get_val("tp_mode", "reference_only") or "reference_only").strip().lower()
    if tp_attach not in {"off", "reference_only", "exchange_tp", "hybrid"}:
        tp_attach = "reference_only"

    return "\n".join([
        "Ã¢ÂšÂ¡ *OxyPulse Pro Control Panel*",


        DIV,


        f"{status_icon} Status: `{status_label}`",


        f"Ã¢ÂšÂ™Ã¯Â¸Â Profile: `{snap['profile_name']}`",


        f"Ã°ÂŸÂ’Â¼ Deploy: `{snap['deployment_mode']}`"


        + (f" -> `{snap['deployment_effective_mode']}`" if snap['deployment_effective_mode'] != snap['deployment_mode'] else ""),


        f"Ã°ÂŸÂ“ÂŠ Exposure: {exposure_line}",


        f"ðŸ’¸ Size: `${snap['base_size']:.0f}` configured Â· `${snap['projected_live_size']:.2f}` live",


        f"Ã°ÂŸÂ“Âˆ Leverage: `{snap['lev_default']}x` configured Ã‚Â· `{snap['live_leverage']}x` live",


        f"Ã°ÂŸÂ§Â  Engines: `{_engine_breakdown_text(settings)}`",


        f"Ã°ÂŸÂ§Â· Protection: {protection}",


        f"Ã°ÂŸÂ“Â¡ Execution: {execution}",


        f"Ã°ÂŸÂŒÂ Market: `{regime}` Ã‚Â· `{session}`",


        f"Ã°ÂŸÂ“Â¦ Open Positions: `{open_positions}`",


        f"Ã°ÂŸÂ”Â„ Sync: `{sync_status}`",


        f"Ã°ÂŸÂ“Â¡ Scanner: `{scanner_status}`",


        "",


        "Open a section below to inspect or change runtime controls.",


        HOT_SCOPE_NOTE,


    ])








def panel_settings_v6core_text(settings) -> str:


    return panel_settings_advanced_text(settings)








def kb_settings_v6core(settings) -> InlineKeyboardMarkup:


    return kb_settings_advanced(settings)








def panel_settings_strategy_text(settings, balance: float | None = None) -> str:


    snap = _strategy_runtime_snapshot(settings, balance=balance)


    return "\n".join([


        "Ã°ÂŸÂ“Â *Strategy*",


        DIV,


        "*Configured*",


        f"Base Size: `${snap['base_size']:.0f}` target notional",


        f"Base Leverage: `{snap['lev_default']}x`",


        f"Min Votes: `{snap['min_votes']:.1f} / 3`",


        f"Exec Mode: `{snap['exec_mode']}`",


        f"Spread Max: `{snap['spread_max']:.2f}%` strategy spread guard",


        f"Confidence: `M {snap['thresholds']['MICRO']:.1f} Ã‚Â· G {snap['thresholds']['GROWTH']:.1f} Ã‚Â· P {snap['thresholds']['PRO']:.1f}`",


        f"Hourly Caps: `M {snap['freq_caps']['MICRO']} Ã‚Â· G {snap['freq_caps']['GROWTH']} Ã‚Â· P {snap['freq_caps']['PRO']}`",


        f"Dynamic Band: `{'ON' if snap['dynamic_sizing_enabled'] else 'OFF'}` -> `${snap['dynamic_target_min_usdt']:.2f}` to `${snap['dynamic_target_max_usdt']:.2f}`",


        "",


        "*Live*",


        f"Profile: `{snap['profile_name']}` on `${snap['balance']:.2f}`",


        f"Intent: `{snap['trade_intent_mode']}` Ã‚Â· {snap['trade_intent_summary']}",


        f"ðŸ’¸ Size: `${snap['base_size']:.0f}` configured Â· `${snap['projected_live_size']:.2f}` live",


        f"Margin Used: `~${snap['margin_used']:.2f}` @ `{snap['live_leverage']}x` live leverage",


        f"Live Cap: `{snap['live_size_cap_pct']:.1f}%` margin budget -> `${snap['margin_cap']:.2f}` max notional",


        f"Live Threshold: `{snap['live_threshold']:.2f}` effective confidence",


        f"Soft Accept: `{snap['soft_accept_threshold']:.2f}` (`{snap['session_name']}`)",
        f"Winrate Opt: `{'OFF' if not bool(settings.get_val('winrate_optimization_enabled', False)) else str(settings.get_val('winrate_profile','balanced') or 'balanced').strip().upper()}`",
        f"Max Trades/hr: `{snap['live_max_trades']}`",


        f"Clamp: `{snap['clamp_reason']}` Ã‚Â· Floor `{snap['exec_floor']:.2f}`",


        f"Risk Reduce: `x{snap['risk_auto_reduce_factor']:.2f}` Ã‚Â· Floor Preserve `{ 'ON' if snap['risk_floor_preserve_enabled'] else 'OFF' }`",
        f"Pre-Reserve Check: `{ 'ON' if snap['pre_reserve_size_validation_enabled'] else 'OFF' }`",


        "",


        "*Field Guide*",


        "Base Size = configured target notional before profile and execution caps.",


        "Live Size = actual executable size after mode, margin, and exchange rules.",


        "Margin Used = approximate initial margin at the live leverage.",


        "Live Cap = per-trade margin budget before exchange contract checks.",


        "Dynamic Band = target live notional window used by dynamic sizing when room exists.",


        "Dynamic Exits = milestone BE/partials + trailing runner (no fixed profit targets).",
        "Pre-Reserve Check rejects known non-executable trades before reserve.",
        "Winrate Opt adjusts ranking and slot fill behavior only (no opaque veto).",
        "Floor Preserve keeps risk-reduced trades executable only in allowed higher-deployment modes.",


        "Min Votes = minimum signal strength before a trade is considered.",


        "Exec Mode = how orders are submitted by the router.",


        "",


        "Participation gates, execution toggles, and market-quality filters live in their own sections below.",


    ])








def kb_settings_strategy(settings, balance: float | None = None) -> InlineKeyboardMarkup:


    snap = _strategy_runtime_snapshot(settings, balance=balance)


    return InlineKeyboardMarkup([


        [InlineKeyboardButton(
            f"Base ${snap['base_size']:.0f} Ã‚Â· Live ${snap['projected_live_size']:.2f}",
            callback_data="stg_size_info",
        ),


         InlineKeyboardButton("Ã¢Â–Â²", callback_data="stg_size_up"),


         InlineKeyboardButton("Ã¢Â–Â¼", callback_data="stg_size_dn")],


        [InlineKeyboardButton(


            f"Lev {snap['lev_default']}x Ã‚Â· Live {snap['live_leverage']}x",


            callback_data="stg_leverage_info",


        ),


         InlineKeyboardButton("Ã¢Â–Â²", callback_data="stg_lev_up"),


         InlineKeyboardButton("Ã¢Â–Â¼", callback_data="stg_lev_dn")],


        [InlineKeyboardButton(


            f"Live Cap {snap['live_size_cap_pct']:.1f}% / ${snap['margin_cap']:.2f}",


            callback_data="stg_livecap_info",


        ),


         InlineKeyboardButton("+", callback_data="stg_livecap_up"),


         InlineKeyboardButton("-", callback_data="stg_livecap_dn")],


        [InlineKeyboardButton(


            f"Dyn Min ${snap['dynamic_target_min_usdt']:.0f}",


            callback_data="stg_dynmin_info",


        ),


         InlineKeyboardButton("+", callback_data="stg_dynmin_up"),


         InlineKeyboardButton("-", callback_data="stg_dynmin_dn")],


        [InlineKeyboardButton(


            f"Dyn Max ${snap['dynamic_target_max_usdt']:.0f}",


            callback_data="stg_dynmax_info",


        ),


         InlineKeyboardButton("+", callback_data="stg_dynmax_up"),


         InlineKeyboardButton("-", callback_data="stg_dynmax_dn")],


        [InlineKeyboardButton(


            f"Votes {snap['min_votes']:.1f} / 3",


            callback_data="stg_votes_info",


        ),


         InlineKeyboardButton("Ã¢Â–Â²", callback_data="stg_votes_up"),


         InlineKeyboardButton("Ã¢Â–Â¼", callback_data="stg_votes_dn")],

        [InlineKeyboardButton(
            f"{'Ã¢Â—Â ' if not bool(settings.get_val('winrate_optimization_enabled', False)) else ''}Off",
            callback_data="stg_wr_off",
        ),
         InlineKeyboardButton(
            f"{'Ã¢Â—Â ' if (bool(settings.get_val('winrate_optimization_enabled', False)) and str(settings.get_val('winrate_profile', 'balanced') or 'balanced').strip().lower() == 'balanced') else ''}Balanced",
            callback_data="stg_wr_bal",
        )],

        [InlineKeyboardButton(
            f"{'Ã¢Â—Â ' if (bool(settings.get_val('winrate_optimization_enabled', False)) and str(settings.get_val('winrate_profile', 'balanced') or 'balanced').strip().lower() == 'strict') else ''}Strict",
            callback_data="stg_wr_strict",
        ),
         InlineKeyboardButton(
            f"{'Ã¢Â—Â ' if (bool(settings.get_val('winrate_optimization_enabled', False)) and str(settings.get_val('winrate_profile', 'balanced') or 'balanced').strip().lower() == 'adaptive') else ''}Adaptive",
            callback_data="stg_wr_adapt",
        )],
        [InlineKeyboardButton(


            f"Exec {snap['exec_mode']}",


            callback_data="stg_exec_cycle_strategy",


        ),


        InlineKeyboardButton(


            "Ã°ÂŸÂŽÂ› Modes",


            callback_data="s:modes",


        )],


        [InlineKeyboardButton("Ã¢ÂšÂ™Ã¯Â¸Â Participation", callback_data="s:participation"),


         InlineKeyboardButton("Ã°ÂŸÂ“Â¡ Execution", callback_data="s:execution")],


        [InlineKeyboardButton("Ã°ÂŸÂ”Â¬ Advanced", callback_data="s:advanced")],


        [InlineKeyboardButton("Ã°ÂŸÂ”Â™ Settings", callback_data="settings_menu")],


    ])








def panel_settings_risk_text(settings, balance: float | None = None, exposure=None) -> str:


    snap = _strategy_runtime_snapshot(settings, balance=balance)


    caps = _active_risk_caps(settings, balance=balance)


    policy = _active_exposure_policy(settings, balance=balance)


    dcap = float(settings.get_val("daily_loss_pct", 12.0))


    wcap = float(settings.get_val("weekly_cap_threshold_pct", settings.get_val("weekly_loss_pct", 25.0)))
    wk_mode = str(settings.get_val("weekly_cap_mode", "adaptive") or "adaptive").strip().lower()
    wk_rec = float(settings.get_val("weekly_cap_recovery_threshold_pct", max(wcap * 0.6, 0.0)) or max(wcap * 0.6, 0.0))
    wk_min_conf = float(settings.get_val("weekly_cap_min_confidence", 4.2) or 4.2)
    wk_red = float(settings.get_val("weekly_cap_size_reduction", 0.5) or 0.5)
    wk_cd = float(settings.get_val("weekly_cap_cooldown_seconds", 120.0) or 120.0)
    rsk = float(settings.get_val("risk_pct", 1.25))


    rule = float(settings.get_val("rule_357_max_open_risk_pct", 25.0))


    max_p = int(settings.get_val("max_open_positions", 10))


    exposure_cap = float(caps.get("exposure_usdt", 0.0) or 0.0)


    exposure_line = "n/a"


    if exposure is not None and snap["balance"] > 0 and exposure_cap > 0:


        current = float(getattr(exposure, "total_usdt", 0.0) or 0.0)


        current_pct = current / snap["balance"] * 100.0 if snap["balance"] else 0.0


        exposure_line = f"`{current_pct:.1f}% / {caps['exposure_pct']:.1f}%`  (`${current:.2f}` / `${exposure_cap:.2f}`)"


    policy_exposure_usdt = snap["balance"] * float(policy.get("total_pct", 0.0) or 0.0) / 100.0 if snap["balance"] > 0 else 0.0


    policy_headroom_usdt = snap["balance"] * float(policy.get("same_side_pct", 0.0) or 0.0) / 100.0 if snap["balance"] > 0 else 0.0


    selected_mode = str(policy.get("selected_mode", policy.get("mode", "Strict")) or "Strict")


    effective_mode = str(policy.get("mode", selected_mode) or selected_mode)


    mode_line = f"`{selected_mode}`"


    if effective_mode != selected_mode:


        mode_line += f" -> `{effective_mode}`"


    warning_line = f"Warning: `{policy['warning']}`" if policy.get("warning") else "Warning: `none`"





    return "\n".join([


        "Ã°ÂŸÂ›Â¡Ã¯Â¸Â *Exposure & Risk*",


        DIV,


        "*Live State*",


        f"Exposure: {exposure_line}",


        f"Base Profile Cap: `{caps['mode']}` exposure `{caps['exposure_pct']:.1f}%` -> `${caps['exposure_usdt']:.2f}`",


        f"Base Same-Direction Headroom: `{caps['headroom_pct']:.1f}%` -> `${caps['headroom_usdt']:.2f}`",


        "",


        "*Exposure Control*",


        f"Mode: {mode_line}",


        f"Effective Exposure: `{policy['total_pct']:.1f}%` -> `${policy_exposure_usdt:.2f}`",


        f"Effective Same-Side Headroom: `{policy['same_side_pct']:.1f}%` -> `${policy_headroom_usdt:.2f}`",


        f"Effective Slots / Correlation: `{int(policy['max_positions'])}` / `{int(policy['correlation_limit'])}`",


        str(policy.get("description") or EXPOSURE_MODE_EXPLANATIONS.get(effective_mode, "")),


        warning_line,


        "",


        f"Risk/Trade: `{rsk:.2f}%`",


        f"Max Open: `{max_p}`",


        f"Open-Risk Cap: `{rule:.1f}%` total",


        f"Daily / Weekly Caps: `{dcap:.0f}% / {wcap:.0f}%`",

        f"Weekly Cap Mode: `{wk_mode.upper()}`",

        f"Weekly Cap Params: `rec={wk_rec:.0f}%` `min_conf={wk_min_conf:.2f}` `reduce={wk_red:.2f}` `cd={wk_cd:.0f}s`",
        f"Drawdown Guard: `{_state_text(settings, 'v6_anti_drawdown_enabled', True)}` _(display-only hard safety)_",


        "Total Exposure Guard: `ACTIVE` _(display-only hard safety)_",


        "",


        "*Field Guide*",


        "Risk/Trade = target account risk allocated to each trade.",


        "Max Open Positions = maximum simultaneous trades allowed.",


        "Open-Risk Cap = combined SL exposure ceiling under the 3-5-7 rule.",


        "Exposure Control changes new-entry tolerance without bypassing stop-loss, sync, or exchange feasibility checks.",


        "Drawdown and total exposure protections stay outside casual toggle control.",


    ])








def kb_settings_risk(settings, balance: float | None = None) -> InlineKeyboardMarkup:


    caps = _active_risk_caps(settings, balance=balance)


    policy = _active_exposure_policy(settings, balance=balance)


    dcap = float(settings.get_val("daily_loss_pct", 12.0))


    wcap = float(settings.get_val("weekly_cap_threshold_pct", settings.get_val("weekly_loss_pct", 25.0)))
    wk_mode = str(settings.get_val("weekly_cap_mode", "adaptive") or "adaptive").strip().lower()
    rsk = float(settings.get_val("risk_pct", 1.25))


    max_p = int(settings.get_val("max_open_positions", 10))


    open_risk = float(settings.get_val("rule_357_max_open_risk_pct", 25.0))


    return InlineKeyboardMarkup([


        [_select_button("Strict", "stg_exposure_mode_strict", str(policy["selected_mode"]) == "Strict"),


         _select_button("Adaptive", "stg_exposure_mode_adaptive", str(policy["selected_mode"]) == "Adaptive")],


        [_select_button("Relaxed", "stg_exposure_mode_relaxed", str(policy["selected_mode"]) == "Relaxed"),


         _select_button("Override", "stg_exposure_mode_override", str(policy["selected_mode"]) == "Override")],


        [InlineKeyboardButton(f"Risk {rsk:.2f}%", callback_data="stg_risk_info"),


         InlineKeyboardButton("Ã¢Â–Â²", callback_data="stg_risk_up"),


         InlineKeyboardButton("Ã¢Â–Â¼", callback_data="stg_risk_dn")],


        [InlineKeyboardButton(f"Max Open {max_p}", callback_data="stg_maxpos_info"),


         InlineKeyboardButton("Ã¢Â–Â²", callback_data="stg_maxpos_up"),


         InlineKeyboardButton("Ã¢Â–Â¼", callback_data="stg_maxpos_dn")],


        [InlineKeyboardButton(f"Open-Risk {open_risk:.1f}%", callback_data="stg_openrisk_info"),


         InlineKeyboardButton("Ã¢Â–Â²", callback_data="stg_openrisk_up"),


         InlineKeyboardButton("Ã¢Â–Â¼", callback_data="stg_openrisk_dn")],


        [InlineKeyboardButton(f"{caps['mode']} Base Exposure {caps['exposure_pct']:.0f}%", callback_data="stg_exp_cap_info"),


         InlineKeyboardButton("+", callback_data="stg_exp_cap_up"),


         InlineKeyboardButton("-", callback_data="stg_exp_cap_dn")],


        [InlineKeyboardButton(f"{caps['mode']} Base Headroom {caps['headroom_pct']:.0f}%", callback_data="stg_headroom_info"),


         InlineKeyboardButton("+", callback_data="stg_headroom_up"),


         InlineKeyboardButton("-", callback_data="stg_headroom_dn")],


        [InlineKeyboardButton(f"Daily Cap {dcap:.0f}%",  callback_data="stg_dcap_info"),


         InlineKeyboardButton("Ã¢Â–Â²", callback_data="stg_dcap_up"),


         InlineKeyboardButton("Ã¢Â–Â¼", callback_data="stg_dcap_dn")],


        [_select_button("W-Off", "stg_wkcap_off", wk_mode == "off"),
         _select_button("Soft", "stg_wkcap_soft", wk_mode == "soft")],

        [_select_button("Strict", "stg_wkcap_strict", wk_mode == "strict"),
         _select_button("Adaptive", "stg_wkcap_adaptive", wk_mode == "adaptive")],

        [InlineKeyboardButton(f"Weekly Cap {wcap:.0f}%", callback_data="stg_wcap_info"),
         InlineKeyboardButton("Ã¢Â–Â²", callback_data="stg_wcap_up"),


         InlineKeyboardButton("Ã¢Â–Â¼", callback_data="stg_wcap_dn")],


        [InlineKeyboardButton("Ã°ÂŸÂ”Â™ Settings", callback_data="settings_menu")],


    ])








def panel_settings_capital_text(


    settings,


    balance: float | None = None,


    exposure=None,


    available_margin: float | None = None,


) -> str:


    snap = _strategy_runtime_snapshot(settings, balance=balance)


    bal = float(snap.get("balance", 0.0) or 0.0)


    current_exposure = float(getattr(exposure, "total_usdt", 0.0) or 0.0) if exposure is not None else 0.0


    exposure_pct = (current_exposure / bal * 100.0) if bal > 0 else 0.0


    allowed_exposure_pct = float(snap.get("deployment_total_exposure_pct", snap.get("profile_exposure_pct", 0.0)) or 0.0)


    allowed_exposure_usdt = bal * allowed_exposure_pct / 100.0 if bal > 0 else 0.0


    same_side_pct = float(snap.get("deployment_same_side_pct", snap.get("direction_cap_pct", 0.0)) or 0.0)


    same_side_usdt = bal * same_side_pct / 100.0 if bal > 0 else 0.0


    long_headroom = short_headroom = 0.0


    try:


        from portfolio_allocator import get_same_direction_headroom_usdt as _headroom





        long_headroom = float(_headroom("long", bal) or 0.0)


        short_headroom = float(_headroom("short", bal) or 0.0)


    except Exception:


        long_headroom = short_headroom = max(same_side_usdt - current_exposure, 0.0)





    profile_setting = str(snap.get("profile_setting", "AUTO") or "AUTO").upper()


    active_profile = str(snap.get("profile_name", "MICRO") or "MICRO").upper()


    profile_line = (


        f"`{profile_setting}` -> `{active_profile}`"


        if profile_setting == "AUTO"


        else f"`{active_profile}` manual"


    )


    deploy_mode = str(snap.get("deployment_mode", "Guarded") or "Guarded")


    deploy_effective = str(snap.get("deployment_effective_mode", deploy_mode) or deploy_mode)


    deploy_line = f"`{deploy_mode}`"


    if deploy_effective != deploy_mode:


        deploy_line += f" -> `{deploy_effective}`"


    global_max = int(settings.get_val("max_open_positions", 10))


    deployment_max = int(snap.get("deployment_max_positions", 0) or 0)


    effective_max = min(global_max, deployment_max) if deployment_max > 0 else global_max


    margin_line = f"${float(available_margin or 0.0):.2f}" if available_margin is not None else "n/a"


    cap_flags = (


        f"Exposure cap `{ 'BASE' if snap.get('deployment_respect_exposure_cap', True) else 'DEPLOY' }`"


        f" Ã‚Â· Headroom `{ 'BASE' if snap.get('deployment_respect_headroom', True) else 'DEPLOY' }`"


    )





    return "\n".join([


        "Ã°ÂŸÂ’Â¼ *Capital & Deployment*",


        DIV,


        f"Capital Profile: {profile_line}",


        f"Deployment Mode: {deploy_line}",


        f"Balance: `${bal:.2f}`",


        f"Available Margin: `{margin_line}`",


        f"Current Exposure: `{exposure_pct:.1f}%` / `{allowed_exposure_pct:.1f}%` (`${current_exposure:.2f}` / `${allowed_exposure_usdt:.2f}`)",


        f"Same-Side Headroom: `L ${long_headroom:.2f}` Ã‚Â· `S ${short_headroom:.2f}` Ã‚Â· cap `{same_side_pct:.1f}%`",


        f"Max Deploy Size: `${float(snap.get('deployment_per_position_target_usdt', 0.0) or 0.0):.2f}` per slot @ `{int(snap.get('deployment_reference_leverage', snap.get('live_leverage', 1)) or 1)}x`",


        f"Deployable Notional: `${float(snap.get('deployment_total_deployable_usdt', 0.0) or 0.0):.2f}` total",


        f"Micro Sizing: `{ 'ON' if snap.get('micro_sizing_active') else 'OFF' }` · Floor `${float(snap.get('effective_account_floor_usdt', 0.0) or 0.0):.2f}` · Buffer `${float(snap.get('effective_margin_buffer_usdt', 0.0) or 0.0):.2f}`",


        f"Max Positions: `mode {deployment_max}` Ã‚Â· `global {global_max}` Ã‚Â· `effective {effective_max}`",


        f"Target Utilization: `{float(snap.get('deployment_target_utilization_pct', 0.0) or 0.0):.1f}%` of balance-driven buying power",


        f"Policy: {cap_flags}",

        "",


        "*Mode Guide*",


        str(snap.get("deployment_description", "Deployment description unavailable")),


        "Exchange available margin is free collateral at OKX.",


        "Bot exposure is a bot-level notional deployment budget and can block before margin is exhausted.",


        HOT_SCOPE_NOTE,


    ])








def kb_settings_capital(settings, balance: float | None = None) -> InlineKeyboardMarkup:


    snap = _strategy_runtime_snapshot(settings, balance=balance)


    profile_setting = str(snap.get("profile_setting", "AUTO") or "AUTO").upper()


    deployment_mode = str(snap.get("deployment_mode", "Guarded") or "Guarded")


    deployment_target = float(snap.get("deployment_target_utilization_pct", 0.0) or 0.0)


    deployment_max = int(snap.get("deployment_max_positions", 0) or 0)


    return InlineKeyboardMarkup([


        [InlineKeyboardButton(f"Profile {profile_setting}", callback_data="stg_cap_profile_cycle")],


        [InlineKeyboardButton(f"Mode {deployment_mode}", callback_data="stg_deploy_mode_cycle")],


        [InlineKeyboardButton(f"Target {deployment_target:.1f}%", callback_data="stg_deploy_target_info"),


         InlineKeyboardButton("+", callback_data="stg_deploy_target_up"),


         InlineKeyboardButton("-", callback_data="stg_deploy_target_dn")],


        [InlineKeyboardButton(f"Mode Max {deployment_max}", callback_data="stg_deploy_maxpos_info"),


         InlineKeyboardButton("+", callback_data="stg_deploy_maxpos_up"),


         InlineKeyboardButton("-", callback_data="stg_deploy_maxpos_dn")],


        [InlineKeyboardButton("Micro Sizing", callback_data="diag:micro_sizing")],


        [InlineKeyboardButton("Ã°ÂŸÂ”Â™ Settings", callback_data="settings_menu")],


    ])








def _live_queue_mode_lines() -> list[str]:
    try:
        from live_state import state as _live_state
        from opportunity_queue import queue as _opportunity_queue
        live_snapshot = _live_state.snapshot()
        queue_snapshot = _opportunity_queue.snapshot(limit=3)
    except Exception:
        live_snapshot = {"state": "UNKNOWN", "armed": False}
        queue_snapshot = {"count": 0, "queued_margin_demand": 0.0, "top": []}
    return [
        "",
        "*Live Arming*",
        f"State: `{live_snapshot.get('state', 'UNKNOWN')}` · Armed: `{'YES' if live_snapshot.get('armed') else 'NO'}`",
        f"Queue: `{int(queue_snapshot.get('count', 0) or 0)}` · Margin demand: `${float(queue_snapshot.get('queued_margin_demand', 0.0) or 0.0):.2f}`",
    ]


def panel_settings_modes_text(


    settings,


    balance: float | None = None,


    exposure=None,


    available_margin: float | None = None,


) -> str:


    snap = _strategy_runtime_snapshot(settings, balance=balance)


    bal = float(snap.get("balance", 0.0) or 0.0)


    current_exposure = float(getattr(exposure, "total_usdt", 0.0) or 0.0) if exposure is not None else 0.0


    exposure_pct = (current_exposure / bal * 100.0) if bal > 0 else 0.0


    allowed_exposure_pct = float(snap.get("deployment_total_exposure_pct", snap.get("profile_exposure_pct", 0.0)) or 0.0)


    allowed_exposure_usdt = bal * allowed_exposure_pct / 100.0 if bal > 0 else 0.0


    same_side_pct = float(snap.get("deployment_same_side_pct", snap.get("direction_cap_pct", 0.0)) or 0.0)


    long_headroom = short_headroom = 0.0


    try:


        from portfolio_allocator import get_same_direction_headroom_usdt as _headroom





        long_headroom = float(_headroom("long", bal) or 0.0)


        short_headroom = float(_headroom("short", bal) or 0.0)


    except Exception:


        long_headroom = short_headroom = max(bal * same_side_pct / 100.0 - current_exposure, 0.0)





    preset_line = _preset_label(settings.get_val("trading_mode_preset", ""))


    profile_setting = str(snap.get("profile_setting", "AUTO") or "AUTO").upper()


    active_profile = str(snap.get("profile_name", "MICRO") or "MICRO").upper()


    profile_line = f"`{profile_setting}`"


    if active_profile != profile_setting:


        profile_line += f" -> `{active_profile}`"





    deploy_mode = str(snap.get("deployment_mode", "Guarded") or "Guarded")


    deploy_effective = str(snap.get("deployment_effective_mode", deploy_mode) or deploy_mode)


    deploy_line = f"`{deploy_mode}`"


    if deploy_effective != deploy_mode:


        deploy_line += f" -> `{deploy_effective}`"





    global_max = int(settings.get_val("max_open_positions", 10))


    deployment_max = int(snap.get("deployment_max_positions", 0) or 0)


    effective_max = min(global_max, deployment_max) if deployment_max > 0 else global_max


    margin_line = f"${float(available_margin or 0.0):.2f}" if available_margin is not None else "n/a"


    cap_flags = (


        f"Exposure `{ 'BASE' if snap.get('deployment_respect_exposure_cap', True) else 'DEPLOY' }`"


        f" Ã‚Â· Headroom `{ 'BASE' if snap.get('deployment_respect_headroom', True) else 'DEPLOY' }`"


    )





    return "\n".join([


        "Ã°ÂŸÂŽÂ› *Modes*",


        DIV,


        "*Trading Preset*",


        f"Preset: `{preset_line}`",


        "Safe = tight protection. Normal = balanced. Aggressive = higher throughput. Full Deployment = Max Deploy + execution-first sizing.",


        "",


        "*Capital Profile*",


        f"Selection: {profile_line}",


        "AUTO follows balance-aware thresholds. MICRO / GROWTH / PRO force that profile directly.",


        "",


        "*Deployment Mode*",


        f"Selection: {deploy_line}",


        str(snap.get("deployment_description", "Deployment description unavailable")),


        "",


        "*Live Capital State*",


        f"Balance: `${bal:.2f}` Ã‚Â· Available Margin: `{margin_line}`",


        f"Exposure: `{exposure_pct:.1f}% / {allowed_exposure_pct:.1f}%` (`${current_exposure:.2f}` / `${allowed_exposure_usdt:.2f}`)",


        f"Same-Side Headroom: `L ${long_headroom:.2f}` Ã‚Â· `S ${short_headroom:.2f}` Ã‚Â· cap `{same_side_pct:.1f}%`",


        f"Max Deploy Size: `${float(snap.get('deployment_per_position_target_usdt', 0.0) or 0.0):.2f}` per slot @ `{int(snap.get('deployment_reference_leverage', snap.get('live_leverage', 1)) or 1)}x`",


        f"Deployable Notional: `${float(snap.get('deployment_total_deployable_usdt', 0.0) or 0.0):.2f}` total",


        f"Max Positions: `mode {deployment_max}` Ã‚Â· `global {global_max}` Ã‚Â· `effective {effective_max}`",


        f"Target Utilization: `{float(snap.get('deployment_target_utilization_pct', 0.0) or 0.0):.1f}%`",


        f"Policy: {cap_flags}",


        "",


        "Exchange available margin is free collateral at OKX.",


        "Bot exposure is a bot-level notional deployment budget and can block before margin is exhausted.",

        *_live_queue_mode_lines(),


        HOT_SCOPE_NOTE,


    ])








def kb_settings_modes(settings, balance: float | None = None) -> InlineKeyboardMarkup:


    snap = _strategy_runtime_snapshot(settings, balance=balance)


    preset = str(settings.get_val("trading_mode_preset", "") or "").strip().lower()


    profile_setting = str(snap.get("profile_setting", "AUTO") or "AUTO").upper()


    deployment_mode = str(snap.get("deployment_mode", "Guarded") or "Guarded")


    deployment_target = float(snap.get("deployment_target_utilization_pct", 0.0) or 0.0)


    deployment_max = int(snap.get("deployment_max_positions", 0) or 0)


    return InlineKeyboardMarkup([


        [InlineKeyboardButton("Live State", callback_data="live:status"),
         InlineKeyboardButton("Live Preflight", callback_data="live:preflight")],
        [InlineKeyboardButton("Opportunity Queue", callback_data="queue:view"),
         InlineKeyboardButton("PnL Reconciliation", callback_data="diag:pnl_sync")],

        [InlineKeyboardButton("Arm Live", callback_data="live:arm_confirm"),
         InlineKeyboardButton("Disarm", callback_data="live:disarm")],

        [InlineKeyboardButton("Halt Admissions", callback_data="live:halt_confirm"),
         InlineKeyboardButton("Emergency Close", callback_data="live:emergency_confirm")],

        [_select_button("Safe", "stg_preset_safe", preset == "safe"),


         _select_button("Balanced", "stg_preset_balanced", preset in {"balanced", "normal"})],
        [_select_button("Aggressive", "stg_preset_aggressive", preset == "aggressive"),


         _select_button("Scalp", "stg_preset_scalp", preset == "scalp")],
        [_select_button("Adaptive", "stg_preset_adaptive", preset in {"adaptive", "ai"})],
        [_select_button("AUTO", "stg_profile_auto", profile_setting == "AUTO"),


         _select_button("MICRO", "stg_profile_micro", profile_setting == "MICRO")],


        [_select_button("GROWTH", "stg_profile_growth", profile_setting == "GROWTH"),


         _select_button("PRO", "stg_profile_pro", profile_setting == "PRO")],


        [_select_button("Guarded", "stg_deploy_guarded", deployment_mode == "Guarded"),


         _select_button("Balanced", "stg_deploy_balanced", deployment_mode == "Balanced")],


        [_select_button("Max Deploy", "stg_deploy_max_deploy", deployment_mode == "Max Deploy"),


         _select_button("Full Deploy", "stg_deploy_full_deployment", deployment_mode == "Full Deployment")],
        [_select_button("Single Shot", "stg_deploy_single_shot", deployment_mode == "Single Shot"),
         _select_button("Swarm", "stg_deploy_swarm", deployment_mode == "Swarm")],
        [_select_button("Recovery", "stg_deploy_recovery", deployment_mode == "Recovery")],
        [InlineKeyboardButton(f"Target {deployment_target:.1f}%", callback_data="stg_deploy_target_info"),


         InlineKeyboardButton("+", callback_data="stg_deploy_target_up"),


         InlineKeyboardButton("-", callback_data="stg_deploy_target_dn")],


        [InlineKeyboardButton(f"Mode Max {deployment_max}", callback_data="stg_deploy_maxpos_info"),


         InlineKeyboardButton("+", callback_data="stg_deploy_maxpos_up"),


         InlineKeyboardButton("-", callback_data="stg_deploy_maxpos_dn")],


        [InlineKeyboardButton("Ã°ÂŸÂ“Â Strategy", callback_data="s:strategy"),


         InlineKeyboardButton("Ã°ÂŸÂ›Â¡Ã¯Â¸Â Risk & Exposure", callback_data="s:risk_cfg")],


        [InlineKeyboardButton("Ã°ÂŸÂ”Â™ Settings", callback_data="settings_menu")],


    ])








def panel_settings_participation_text(settings) -> str:


    summary = " Ã‚Â· ".join([


        f"Soft `{_state_text(settings, 'soft_accept_enabled', True)}`",


        f"Lane `{_state_text(settings, 'lane_budget_enabled', True)}`",


        f"Override `{_state_text(settings, 'lane_override_enabled', True)}`",


        f"Confirm `{_state_text(settings, 'confirm_path_enabled', True)}`",


    ])





    return "\n".join([


        "Ã¢ÂšÂ™Ã¯Â¸Â *Participation Controls*",


        DIV,


        f"`{summary}`",


        HOT_SCOPE_NOTE,


        "",


        "*Signal Qualification*",


        _control_line(settings, "soft_accept_enabled", "Soft Accept", PARTICIPATION_EXPLANATIONS["soft_accept_enabled"], True),


        _control_line(settings, "entry_filter_enabled", "Entry Filter", PARTICIPATION_EXPLANATIONS["entry_filter_enabled"], True),


        _control_line(settings, "htf_filter_enabled", "HTF Filter", PARTICIPATION_EXPLANATIONS["htf_filter_enabled"], True),


        _control_line(settings, "session_filter_enabled", "Session Filter", PARTICIPATION_EXPLANATIONS["session_filter_enabled"], True),


        "",


        "*Allocation / Selection*",


        _control_line(settings, "lane_budget_enabled", "Lane Budget", PARTICIPATION_EXPLANATIONS["lane_budget_enabled"], True),


        _control_line(settings, "lane_override_enabled", "Lane Override", PARTICIPATION_EXPLANATIONS["lane_override_enabled"], True),


        _control_line(settings, "same_direction_headroom_enabled", "Same-Direction Headroom", PARTICIPATION_EXPLANATIONS["same_direction_headroom_enabled"], True),


        _control_line(settings, "correlation_filter_enabled", "Correlation Filter", PARTICIPATION_EXPLANATIONS["correlation_filter_enabled"], True),


        _control_line(settings, "per_engine_cap_enabled", "Per-Engine Cap", PARTICIPATION_EXPLANATIONS["per_engine_cap_enabled"], True),


        _control_line(settings, "engine_weight_adaptation_enabled", "Engine Weight Adaptation", PARTICIPATION_EXPLANATIONS["engine_weight_adaptation_enabled"], False),


        "",


        "*Flow / Conversion*",


        _control_line(settings, "confirm_path_enabled", "Confirm Path", PARTICIPATION_EXPLANATIONS["confirm_path_enabled"], True),


        _control_line(settings, "safe_reentry_enabled", "Safe Re-entry", PARTICIPATION_EXPLANATIONS["safe_reentry_enabled"], True),


    ])








def kb_settings_participation(settings) -> InlineKeyboardMarkup:


    def btn(label, key):


        state = "Ã¢ÂœÂ…" if settings.get_val(key) else "Ã¢ÂÂŒ"


        return InlineKeyboardButton(f"{state} {label}", callback_data=f"stg_tog_{key}")





    return InlineKeyboardMarkup([


        [btn("Soft Accept", "soft_accept_enabled"),


         btn("Entry Filter", "entry_filter_enabled")],


        [btn("HTF Filter", "htf_filter_enabled"),


         btn("Session Filter", "session_filter_enabled")],


        [btn("Lane Budget", "lane_budget_enabled"),


         btn("Lane Override", "lane_override_enabled")],


        [btn("Headroom", "same_direction_headroom_enabled"),


         btn("Correlation", "correlation_filter_enabled")],


        [btn("Engine Cap", "per_engine_cap_enabled"),


         btn("Weight Adapt", "engine_weight_adaptation_enabled")],


        [btn("Confirm Path", "confirm_path_enabled"),


         btn("Safe Re-entry", "safe_reentry_enabled")],


        [InlineKeyboardButton("Ã°ÂŸÂ”Â™ Settings", callback_data="settings_menu")],


    ])








def panel_settings_execution_text(settings) -> str:
    cooldown_secs = float(settings.get_val("preflight_zero_contract_cooldown_secs", 1800.0) or 1800.0)


    cooldown_window = float(settings.get_val("preflight_zero_contract_window_secs", 1800.0) or 1800.0)


    trigger_count = int(settings.get_val("preflight_zero_contract_trigger_count", 2) or 2)


    exec_mode = str(settings.get_val("v6_execution_mode", "market")).upper()


    reduce_factor = float(settings.get_val("risk_auto_reduce_factor", 0.50) or 0.50)


    secure_target = float(settings.get_val("secure_profit_target_usd", 3.0) or 3.0)


    be_profit = float(settings.get_val("breakeven_activation_profit_usd", 0.35) or 0.35)


    trail_profit = float(settings.get_val("trail_activation_profit_usd", 0.90) or 0.90)


    partial_tp1 = float(settings.get_val("partial_tp1_close_pct", 30.0) or 30.0)


    partial_tp2 = float(settings.get_val("partial_tp2_close_pct", 20.0) or 20.0)


    min_profit_target = float(settings.get_val("min_profit_target_usdt", 2.0) or 2.0)


    tp_mode = str(settings.get_val("tp_distance_mode", "balanced") or "balanced").lower()

    tp_attach = str(settings.get_val("tp_mode", "reference_only") or "reference_only").strip().lower()
    if tp_attach not in {"off", "reference_only", "exchange_tp", "hybrid"}:
        tp_attach = "reference_only"
    tp1_scale, tp2_scale = (


        settings.get_tp_distance_scales(tp_mode)


        if hasattr(settings, "get_tp_distance_scales")


        else (1.0, 1.0)


    )


    return "\n".join([


        "Ã°ÂŸÂ“Â¡ *Execution*",


        DIV,


        _control_line(settings, "dynamic_sizing_enabled", "Dynamic Sizing", EXECUTION_EXPLANATIONS["dynamic_sizing_enabled"], True),


        _control_line(settings, "leverage_fallback_enabled", "Leverage Fallback", EXECUTION_EXPLANATIONS["leverage_fallback_enabled"], True),


        "Fallback Ladder: `15x -> 10x -> 5x`",


        _control_line(settings, "preflight_cooldown_enabled", "Preflight Cooldown", EXECUTION_EXPLANATIONS["preflight_cooldown_enabled"], True),


        _control_line(settings, "pre_reserve_size_validation_enabled", "Pre-Reserve Validation", EXECUTION_EXPLANATIONS["pre_reserve_size_validation_enabled"], True),


        _control_line(settings, "risk_floor_preserve_enabled", "Floor Preserve", EXECUTION_EXPLANATIONS["risk_floor_preserve_enabled"], True),


        f"Drawdown Reduce Factor: `x{reduce_factor:.2f}` during active auto-reduce",


        f"Cooldown Window: `{cooldown_window/60.0:.0f}m` | Trigger: `{trigger_count}` failures | Cooldown: `{cooldown_secs/60.0:.0f}m`",


        f"Exec Mode: `{exec_mode}`",


        _control_line(settings, "v6_execution_allow_limit", "Limit Orders", EXECUTION_EXPLANATIONS["v6_execution_allow_limit"], False),


        _control_line(settings, "v6_execution_allow_staged", "Staged Entry", EXECUTION_EXPLANATIONS["v6_execution_allow_staged"], False),


        _control_line(settings, "secure_profit_mode_enabled", "Dynamic Exits", EXECUTION_EXPLANATIONS["secure_profit_mode_enabled"], True),
        (
            f"Milestones (ROI): `BE {float(settings.get_val('breakeven_trigger_pct', 1.1) or 1.1):.1f}%`"
            f" | `TP1 {float(settings.get_val('tp1_trigger_pct', 1.8) or 1.8):.1f}% -> {partial_tp1:.0f}%`"
            f" | `TP2 {float(settings.get_val('tp2_trigger_pct', 3.8) or 3.8):.1f}% -> {partial_tp2:.0f}%`"
        ),
        (
            f"Protection: `Lock {float(settings.get_val('profit_lock_trigger_pct', 2.2) or 2.2):.1f}%`"
            f" | `Trail {float(settings.get_val('trailing_activation_pct', 3.0) or 3.0):.1f}%`"
            f" | `Min TP ${float(settings.get_val('min_net_take_profit_usdt', 0.05) or 0.05):.2f}`"
        ),
        f"TP Attach: `{tp_attach.upper()}`",
        f"Effective TP Owner (new): `{derive_tp_owner(tp_attach, partial_enabled=bool(settings.get_val('partial_exit_enabled', True)))}`",
        f"TP Mode: `{tp_mode.upper()}` | TP1 x`{tp1_scale:.2f}` | TP2 x`{tp2_scale:.2f}`",
        f"Partials: TP1 `{partial_tp1:.0f}%` | TP2 `{partial_tp2:.0f}%`",
        _control_line(settings, "breakeven_enabled", "Breakeven", EXECUTION_EXPLANATIONS["breakeven_enabled"], True),


        _control_line(settings, "partial_exit_enabled", "Partial Exit", EXECUTION_EXPLANATIONS["partial_exit_enabled"], True),


        _control_line(settings, "tsl_enabled", "Runner Trail", EXECUTION_EXPLANATIONS["tsl_enabled"], True),
        "",


        HOT_SCOPE_NOTE,


    ])








def kb_settings_execution(settings) -> InlineKeyboardMarkup:
    def btn(label, key):


        state = "ON" if settings.get_val(key) else "OFF"


        return InlineKeyboardButton(f"{state} {label}", callback_data=f"stg_tog_{key}")





    cooldown_mins = float(settings.get_val("preflight_zero_contract_cooldown_secs", 1800.0) or 1800.0) / 60.0


    trigger_count = int(settings.get_val("preflight_zero_contract_trigger_count", 2) or 2)


    exec_mode = str(settings.get_val("v6_execution_mode", "market")).upper()


    secure_target = float(settings.get_val("secure_profit_target_usd", 3.0) or 3.0)


    min_profit_target = float(settings.get_val("min_profit_target_usdt", 2.0) or 2.0)


    reduce_factor = float(settings.get_val("risk_auto_reduce_factor", 0.50) or 0.50)


    partial_tp1 = float(settings.get_val("partial_tp1_close_pct", 30.0) or 30.0)


    partial_tp2 = float(settings.get_val("partial_tp2_close_pct", 20.0) or 20.0)


    tp_mode = str(settings.get_val("tp_distance_mode", "balanced") or "balanced").upper()
    tp_attach = str(settings.get_val("tp_mode", "reference_only") or "reference_only").strip().lower()
    if tp_attach not in {"off", "reference_only", "exchange_tp", "hybrid"}:
        tp_attach = "reference_only"
    tp_attach_label = {
        "off": "OFF",
        "reference_only": "REF",
        "exchange_tp": "EXCH",
        "hybrid": "HYBRID",
    }.get(tp_attach, tp_attach.upper())
    return InlineKeyboardMarkup([


        [btn("Dyn Sizing", "dynamic_sizing_enabled"),


         btn("Lev Fallback", "leverage_fallback_enabled")],
        [btn("Cooldown", "preflight_cooldown_enabled")],
        [btn("Floor Preserve", "risk_floor_preserve_enabled"),


         btn("Pre-Reserve", "pre_reserve_size_validation_enabled")],


        [InlineKeyboardButton(f"DD Reduce x{reduce_factor:.2f}", callback_data="stg_risk_reduce_info"),


         InlineKeyboardButton("+", callback_data="stg_risk_reduce_up"),


         InlineKeyboardButton("-", callback_data="stg_risk_reduce_dn")],


        [InlineKeyboardButton(f"Exec {exec_mode}", callback_data="stg_exec_cycle")],


        [btn("Limit Orders", "v6_execution_allow_limit"),


         btn("Staged Entry", "v6_execution_allow_staged")],


        [btn("Dynamic Exits", "secure_profit_mode_enabled")],
        [InlineKeyboardButton(f"TP Attach {tp_attach_label}", callback_data="stg_tp_attach_cycle")],
        [InlineKeyboardButton(f"TP {tp_mode}", callback_data="stg_tp_mode_cycle")],
        [btn("Breakeven", "breakeven_enabled"),
         btn("Partial Exit", "partial_exit_enabled")],


        [btn("Runner Trail", "tsl_enabled")],
        [InlineKeyboardButton(f"TP1 {partial_tp1:.0f}%", callback_data="stg_ptp1_info"),


         InlineKeyboardButton("+", callback_data="stg_ptp1_up"),


         InlineKeyboardButton("-", callback_data="stg_ptp1_dn")],


        [InlineKeyboardButton(f"TP2 {partial_tp2:.0f}%", callback_data="stg_ptp2_info"),


         InlineKeyboardButton("+", callback_data="stg_ptp2_up"),


         InlineKeyboardButton("-", callback_data="stg_ptp2_dn")],


        [InlineKeyboardButton(f"Cooldown {cooldown_mins:.0f}m", callback_data="stg_pf_cool_info"),


         InlineKeyboardButton("+", callback_data="stg_pf_cool_up"),


         InlineKeyboardButton("-", callback_data="stg_pf_cool_dn")],


        [InlineKeyboardButton(f"Trigger {trigger_count}", callback_data="stg_pf_trigger_info"),


         InlineKeyboardButton("+", callback_data="stg_pf_trigger_up"),


         InlineKeyboardButton("-", callback_data="stg_pf_trigger_dn")],


        [InlineKeyboardButton("Ã°ÂŸÂ§Âª Filters & Quality", callback_data="s:filters"),


         InlineKeyboardButton("Ã°ÂŸÂ”Â¬ Advanced", callback_data="s:advanced")],


        [InlineKeyboardButton("Ã°ÂŸÂ”Â™ Settings", callback_data="settings_menu")],


    ])








def panel_settings_filters_text(settings) -> str:


    spread_pct = float(settings.get_val("v6_spread_max_pct", 0.30) or 0.30)


    min_volume = float(settings.get_val("v6_min_volume_usd_24h", 2_000_000.0) or 2_000_000.0)


    return "\n".join([


        "Ã°ÂŸÂ§Âª *Filters & Market Quality*",


        DIV,


        _control_line(settings, "spread_filter_enabled", "Spread Filter", FILTER_EXPLANATIONS["spread_filter_enabled"], True),


        f"Active Max Spread: `{spread_pct:.2f}%` for live market admission",


        f"Active Min Volume: `{_fmt_compact_usd(min_volume)}` 24h",


        _control_line(settings, "dead_coin_suppression_enabled", "Dead-Coin Suppression", FILTER_EXPLANATIONS["dead_coin_suppression_enabled"], True),


        _control_line(settings, "executability_penalty_enabled", "Executability Penalty", FILTER_EXPLANATIONS["executability_penalty_enabled"], True),


        _control_line(settings, "market_cap_bonus_enabled", "Market-Cap Bonus", FILTER_EXPLANATIONS["market_cap_bonus_enabled"], True),


        _control_line(settings, "small_account_large_cap_preference_enabled", "Small-Account Large-Cap Preference", FILTER_EXPLANATIONS["small_account_large_cap_preference_enabled"], True),


        "",


        "These controls drive the live market filter used for new decisions.",


        "Volume floor: `$0` disables volume blocking. Decrements get finer below `$1.0M`.",


        "All discovery engines still scan the same broad tradable universe.",


        HOT_SCOPE_NOTE,


    ])








def kb_settings_filters(settings) -> InlineKeyboardMarkup:


    def btn(label, key):


        state = "Ã¢ÂœÂ…" if settings.get_val(key) else "Ã¢ÂÂŒ"


        return InlineKeyboardButton(f"{state} {label}", callback_data=f"stg_tog_{key}")





    spread_pct = float(settings.get_val("v6_spread_max_pct", 0.30) or 0.30)


    min_volume = float(settings.get_val("v6_min_volume_usd_24h", 2_000_000.0) or 2_000_000.0)


    return InlineKeyboardMarkup([


        [btn("Spread Filter", "spread_filter_enabled"),


         btn("Dead-Coin Supp", "dead_coin_suppression_enabled")],


        [btn("Executability", "executability_penalty_enabled"),


         btn("MCap Bonus", "market_cap_bonus_enabled")],


        [btn("Small-Account Bonus", "small_account_large_cap_preference_enabled")],


        [InlineKeyboardButton(f"Spread {spread_pct:.2f}%", callback_data="stg_uspread_info"),


         InlineKeyboardButton("Ã¢Â–Â²", callback_data="stg_uspread_up"),


         InlineKeyboardButton("Ã¢Â–Â¼", callback_data="stg_uspread_dn")],


        [InlineKeyboardButton(f"Min Vol {_fmt_compact_usd(min_volume)}", callback_data="stg_uvol_info"),


         InlineKeyboardButton("Ã¢Â–Â²", callback_data="stg_uvol_up"),


         InlineKeyboardButton("Ã¢Â–Â¼", callback_data="stg_uvol_dn")],


        [InlineKeyboardButton("Ã°ÂŸÂ”Â™ Settings", callback_data="settings_menu")],


    ])








def _norm_choice(value: str | None, allowed: set[str], default: str) -> str:
    raw = str(value or default).strip().lower()
    return raw if raw in allowed else default


def panel_settings_slots_text(settings, balance: float | None = None) -> str:
    bal = max(float(balance or 0.0), 0.0)
    slot_mode = _norm_choice(settings.get_val("global_slot_mode", "fixed"), {"fixed", "auto"}, "fixed")
    selection = _norm_choice(
        settings.get_val("trade_selection_policy", "replace_weakest"),
        {"first_come", "best_score", "replace_weakest"},
        "replace_weakest",
    )
    max_pos = int(settings.get_val("max_open_positions", 10) or 10)

    used = 0
    try:
        import position_registry as _pr
        reg = getattr(_pr, "_registry", {}) or {}
        used = len(reg)
    except Exception:
        used = 0

    cap_line = "Effective Slots: `Ã¢Â€Â”`"
    if bal > 0 and bool(settings.get_val("v6_enabled", False)):
        try:
            from capital_manager import get_profile as _get_profile
            prof = _get_profile(bal)
        except Exception:
            prof = None
        try:
            import category_engine as _ce
            cap, meta = _ce.get_effective_global_slot_limit(bal, capital_profile=prof)
            cap_line = f"Effective Slots: `{used}/{int(cap)}`  (mode `{meta.get('global_slot_mode','?')}`)"
        except Exception:
            cap_line = f"Effective Slots: `{used}/{max_pos}`"

    return "\n".join([
        "Ã°ÂŸÂ§Â· *Slots & Allocation*",
        DIV,
        f"Global Slot Mode: `{slot_mode}`",
        f"Max Positions (fixed): `{max_pos}`",
        cap_line,
        f"Selection Policy: `{selection}`",
        "",
        "Fixed/auto slot controls affect NEW entries only.",
    ])


def kb_settings_slots(settings, balance: float | None = None) -> InlineKeyboardMarkup:
    slot_mode = _norm_choice(settings.get_val("global_slot_mode", "fixed"), {"fixed", "auto"}, "fixed")
    selection = _norm_choice(
        settings.get_val("trade_selection_policy", "replace_weakest"),
        {"first_come", "best_score", "replace_weakest"},
        "replace_weakest",
    )
    max_pos = int(settings.get_val("max_open_positions", 10) or 10)

    def pick(label: str, selected: bool, cb: str) -> InlineKeyboardButton:
        return InlineKeyboardButton(f"{'Ã¢ÂœÂ…' if selected else 'Ã‚Â·'} {label}", callback_data=cb)

    return InlineKeyboardMarkup([
        [
            pick("Fixed", slot_mode == "fixed", "stg_global_slot_fixed"),
            pick("Auto", slot_mode == "auto", "stg_global_slot_auto"),
        ],
        [
            InlineKeyboardButton(f"Max Pos {max_pos}", callback_data="stg_slots_info"),
            InlineKeyboardButton("+", callback_data="stg_slots_up"),
            InlineKeyboardButton("-", callback_data="stg_slots_dn"),
        ],
        [
            pick("First", selection == "first_come", "stg_sel_first"),
            pick("Best", selection == "best_score", "stg_sel_best"),
            pick("Replace", selection == "replace_weakest", "stg_sel_replace"),
        ],
        [InlineKeyboardButton("Ã°ÂŸÂ§Â  Engine Slots", callback_data="s:slots_engines:0")],
        [InlineKeyboardButton("Ã°ÂŸÂ”Â™ Settings", callback_data="settings_menu")],
    ])


_ENGINE_SLOTS_PAGE_SIZE = 10


def _engine_slot_mode(settings, engine_name: str) -> str:
    name = str(engine_name or "").upper()
    raw = str(settings.get_val(f"engine_{name.lower()}_slot_mode", "") or settings.get_val("engine_slot_mode", "fixed"))
    raw = raw.strip().lower()
    if raw in {"fixed", "inherit", "unlimited"}:
        return raw
    _raw_max = settings.get_val(f"engine_{name.lower()}_max_pos", 3)
    try:
        max_pos = int(_raw_max) if _raw_max is not None else 3
    except Exception:
        max_pos = 3
    if max_pos == 0:
        return "inherit"
    if max_pos < 0:
        return "unlimited"
    return "fixed"


def panel_settings_engine_slots_text(settings, balance: float | None = None, page: int = 0) -> str:
    bal = max(float(balance or 0.0), 0.0)
    used_by_engine: dict[str, int] = {}
    try:
        import position_registry as _pr
        reg = getattr(_pr, "_registry", {}) or {}
        for v in reg.values():
            eng = str(v.get("engine", "") or "").upper() or "?"
            used_by_engine[eng] = used_by_engine.get(eng, 0) + 1
    except Exception:
        used_by_engine = {}

    try:
        import settings_store as _ss
        engines = sorted(list(getattr(_ss, "ENGINE_LANES", {}).keys()))
    except Exception:
        engines = []

    page = max(int(page or 0), 0)
    start = page * _ENGINE_SLOTS_PAGE_SIZE
    subset = engines[start:start + _ENGINE_SLOTS_PAGE_SIZE]

    lines = ["Ã°ÂŸÂ§Â  *Engine Slots*", DIV]
    if not subset:
        lines.append("_No engines found._")
        return "\n".join(lines)

    prof = None
    if bal > 0 and bool(settings.get_val("v6_enabled", False)):
        try:
            from capital_manager import get_profile as _get_profile
            prof = _get_profile(bal)
        except Exception:
            prof = None

    for eng in subset:
        mode = _engine_slot_mode(settings, eng)
        _raw_cfg = settings.get_val(f"engine_{eng.lower()}_max_pos", 3)
        try:
            cfg = int(_raw_cfg) if _raw_cfg is not None else 3
        except Exception:
            cfg = 3
        used = int(used_by_engine.get(eng.upper(), 0) or 0)
        eff = "Ã¢Â€Â”"
        if bal > 0:
            try:
                import category_engine as _ce
                eff = str(int(_ce.get_effective_engine_cap(eng, balance=bal, capital_profile=prof) or 0))
            except Exception:
                eff = "Ã¢Â€Â”"
        lines.append(f"`{eng}`  used `{used}` Ã‚Â· cfg `{cfg}` Ã‚Â· mode `{mode}` Ã‚Â· eff `{eff}`")

    lines.append("")
    lines.append(f"Page `{page+1}`")
    return "\n".join(lines)


def kb_settings_engine_slots(settings, page: int = 0) -> InlineKeyboardMarkup:
    try:
        import settings_store as _ss
        engines = sorted(list(getattr(_ss, "ENGINE_LANES", {}).keys()))
    except Exception:
        engines = []

    page = max(int(page or 0), 0)
    start = page * _ENGINE_SLOTS_PAGE_SIZE
    subset = engines[start:start + _ENGINE_SLOTS_PAGE_SIZE]

    rows = []
    for i in range(0, len(subset), 2):
        a = subset[i]
        b = subset[i + 1] if i + 1 < len(subset) else None
        row = [InlineKeyboardButton(a, callback_data=f"s:slots_engine:{a}:{page}")]
        if b:
            row.append(InlineKeyboardButton(b, callback_data=f"s:slots_engine:{b}:{page}"))
        rows.append(row)

    nav = []
    if start > 0:
        nav.append(InlineKeyboardButton("Ã¢Â†Â Prev", callback_data=f"s:slots_engines:{page-1}"))
    if start + _ENGINE_SLOTS_PAGE_SIZE < len(engines):
        nav.append(InlineKeyboardButton("Next Ã¢Â†Â’", callback_data=f"s:slots_engines:{page+1}"))
    if nav:
        rows.append(nav)

    rows.append([InlineKeyboardButton("Ã¢Â†Â Slots", callback_data="s:slots")])
    return InlineKeyboardMarkup(rows)


def panel_settings_engine_slot_detail_text(settings, engine_name: str, balance: float | None = None) -> str:
    bal = max(float(balance or 0.0), 0.0)
    eng = str(engine_name or "").upper().strip() or "?"
    _raw_cfg = settings.get_val(f"engine_{eng.lower()}_max_pos", 3)
    try:
        cfg = int(_raw_cfg) if _raw_cfg is not None else 3
    except Exception:
        cfg = 3
    mode = _engine_slot_mode(settings, eng)

    used = 0
    try:
        import position_registry as _pr
        reg = getattr(_pr, "_registry", {}) or {}
        used = sum(1 for v in reg.values() if str(v.get("engine", "") or "").upper() == eng)
    except Exception:
        used = 0

    eff = "Ã¢Â€Â”"
    if bal > 0 and bool(settings.get_val("v6_enabled", False)):
        try:
            from capital_manager import get_profile as _get_profile
            prof = _get_profile(bal)
        except Exception:
            prof = None
        try:
            import category_engine as _ce
            eff = str(int(_ce.get_effective_engine_cap(eng, balance=bal, capital_profile=prof) or 0))
        except Exception:
            eff = "Ã¢Â€Â”"

    return "\n".join([
        f"Ã°ÂŸÂ§Â  *Engine Slots Ã¢Â€Â” {eng}*",
        DIV,
        f"Config: `{cfg}` Ã‚Â· Mode: `{mode}`",
        f"Usage: `{used}/{eff}`",
        "",
        "Modes: fixed / inherit / unlimited (bounded).",
    ])


def kb_settings_engine_slot_detail(settings, engine_name: str, back_page: int = 0) -> InlineKeyboardMarkup:
    eng = str(engine_name or "").upper().strip() or "?"
    mode = _engine_slot_mode(settings, eng)
    _raw_cfg = settings.get_val(f"engine_{eng.lower()}_max_pos", 3)
    try:
        cfg = int(_raw_cfg) if _raw_cfg is not None else 3
    except Exception:
        cfg = 3

    def sel(label: str, active: bool, cb: str) -> InlineKeyboardButton:
        return InlineKeyboardButton(f"{'Ã¢ÂœÂ…' if active else 'Ã‚Â·'} {label}", callback_data=cb)

    return InlineKeyboardMarkup([
        [
            sel("Inherit", mode == "inherit" or cfg == 0, f"stg_engslot:{eng}:inherit:{int(back_page or 0)}"),
            sel("Unlimited", mode == "unlimited" or cfg < 0, f"stg_engslot:{eng}:unlimited:{int(back_page or 0)}"),
        ],
        [
            InlineKeyboardButton("Ã¢ÂˆÂ’", callback_data=f"stg_engslot:{eng}:dn:{int(back_page or 0)}"),
            InlineKeyboardButton(f"CAP {cfg if cfg > 0 else 1}", callback_data=f"stg_engslot:{eng}:info:{int(back_page or 0)}"),
            InlineKeyboardButton("+", callback_data=f"stg_engslot:{eng}:up:{int(back_page or 0)}"),
        ],
        [InlineKeyboardButton("Ã¢Â†Â Engines", callback_data=f"s:slots_engines:{int(back_page or 0)}")],
    ])


def panel_settings_engines_text(settings) -> str:
    counts = _engine_mode_breakdown(settings)


    return "\n".join([


        "Ã°ÂŸÂ§Â  *Engines*",


        DIV,


        f"Mode Mix: `{counts['live']} LIVE Ã‚Â· {counts['shadow']} SHADOW Ã‚Â· {counts['disabled']} DISABLED Ã‚Â· {counts['total']} TOTAL`",


        "Tap an engine below to cycle: `DISABLED Ã¢Â†Â’ SHADOW Ã¢Â†Â’ PROBATION Ã¢Â†Â’ LIVE`.",


        "Probation places real trades with closer monitoring and counts inside LIVE.",


        "Every engine mode uses the canonical `engine_<name>_mode` key only.",


    ])








def panel_settings_ai_text(settings) -> str:


    return "\n".join([


        "Ã°ÂŸÂ¤Â– *AI / Automation*",


        DIV,


        _control_line(settings, "v6_ai_veto_enabled", "AI Gatekeeper", AI_EXPLANATIONS["v6_ai_veto_enabled"], False),


        _control_line(settings, "v6_telemetry_enabled", "Telemetry", AI_EXPLANATIONS["v6_telemetry_enabled"], True),


        _control_line(settings, "automation_mutation_enabled", "Automation Mutations", AI_EXPLANATIONS["automation_mutation_enabled"], False),


        "",


        "Proactive AI checks run on the bot job queue while the process is alive. _(display-only)_",


        "Engine adaptation stays under Participation because it directly changes trade selection.",


        HOT_SCOPE_NOTE,


    ])








def kb_settings_ai(settings) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Control State", callback_data="diag:control_state")],
        [InlineKeyboardButton("Telemetry", callback_data="set:room:v6_telemetry_enabled")],
        [InlineKeyboardButton("Automation Mutations", callback_data="set:room:automation_mutation_enabled")],
        [InlineKeyboardButton("? Settings", callback_data="settings_menu")],
    ])

def panel_fg_size_text(settings) -> str:
    mode = str(settings.get_val("size_mode", "blended") or "blended").strip().lower()
    return "\n".join([
        "Ã°ÂŸÂ’Âµ *Size Controls*",
        DIV,
        f"Size Mode: `{mode}`",
        "Base Trade Size, Balance % Size, and Risk % Size all affect NEW entries.",
        "Unlimited / No Cap is not used here; size controls remain visible and auditable.",
    ])


def kb_fg_size(settings) -> InlineKeyboardMarkup:
    mode = str(settings.get_val("size_mode", "blended") or "blended").strip().lower()
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Base Trade Size", callback_data="set:room:position_size_usdt"),
         InlineKeyboardButton("Min Trade Size", callback_data="set:room:v6_min_position_size_usdt")],
        [InlineKeyboardButton("Max Trade Size", callback_data="set:room:v6_max_position_size_usdt"),
         InlineKeyboardButton("Balance % Size", callback_data="set:room:position_size_pct")],
        [InlineKeyboardButton("Risk % Size", callback_data="set:room:risk_pct"),
         InlineKeyboardButton("Dynamic Sizing", callback_data="set:room:dynamic_sizing_enabled")],
        [InlineKeyboardButton(f"Size Mode {mode}", callback_data="set:room:size_mode")],
        [InlineKeyboardButton("Ã°ÂŸÂ”Â™ Capital", callback_data="set:cat:capital")],
        [InlineKeyboardButton("Ã°ÂŸÂ”Â™ Settings", callback_data="settings_menu")],
    ])


def panel_settings_cat_capital_text(settings) -> str:
    return "\n".join([
        "Ã°ÂŸÂ“Â‰ *Settings Ã‚Â· Capital*",
        DIV,
        "Deployment posture, allocation, and trade sizing controls.",
    ])


def kb_settings_cat_capital(settings) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Ã°ÂŸÂ’Â° Deployment", callback_data="set:fg:deployment")],
        [InlineKeyboardButton("Ã°ÂŸÂ’Âµ Size", callback_data="set:fg:size")],
        [InlineKeyboardButton("Ã¢Â†Â Settings", callback_data="settings_menu")],
    ])


def panel_settings_cat_ai_text(settings) -> str:
    return "\n".join([
        "Ã°ÂŸÂ¤Â– *Settings Ã‚Â· AI*",
        DIV,
        "Control state and runtime override visibility.",
    ])


def kb_settings_cat_ai(settings) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Ã°ÂŸÂ¤Â– AI", callback_data="set:fg:ai_gate")],
        [InlineKeyboardButton("Ã¢Â†Â Settings", callback_data="settings_menu")],
    ])


def panel_settings_cat_diagnostics_text(settings) -> str:
    return "\n".join([
        "Ã°ÂŸÂ§Âª *Settings Ã‚Â· Diagnostics*",
        DIV,
        "Runtime visibility, traces, and diagnostic override controls.",
    ])


def kb_settings_cat_diagnostics(settings) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Ã°ÂŸÂ§Â¾ Decision Trace", callback_data="diag:decision_trace"),
         InlineKeyboardButton("Ã°ÂŸÂ“Â¡ API Health", callback_data="diag:api_health")],
        [InlineKeyboardButton("Ã°ÂŸÂŽÂ› Control State", callback_data="diag:control_state"),
         InlineKeyboardButton("Ã°ÂŸÂ§Â  Engines", callback_data="diag:engines")],
        [InlineKeyboardButton("Shadow Rehab", callback_data="diag:shadow_rehab"),
         InlineKeyboardButton("IZANAGI Outcomes", callback_data="diag:izanagi_outcomes")],
        [InlineKeyboardButton("Ã°ÂŸÂ“Â¡ Telemetry", callback_data="set:room:v6_telemetry_enabled"),
         InlineKeyboardButton("Ã°ÂŸÂ§Â¾ Block Detail", callback_data="set:room:block_diagnostics_enabled")],
        [InlineKeyboardButton("Ã°ÂŸÂ§Â¯ Debug Overrides", callback_data="diag:overrides")],
        [InlineKeyboardButton("Ã¢Â†Â Settings", callback_data="settings_menu")],
    ])


def panel_settings_diagnostics_text(settings, balance: float | None = None) -> str:
    return panel_diagnostics_text(settings, balance=balance)


def kb_settings_diagnostics(settings, balance: float | None = None) -> InlineKeyboardMarkup:
    return kb_diagnostics(settings)


def panel_control_state_text(settings, balance: float | None = None) -> str:
    try:
        import settings_store as _ss
        snap = _ss.get_debug_override_snapshot() or {}
    except Exception:
        snap = {"mode": "off", "active": {}}
    dbg_mode = str(snap.get("mode", "off") or "off").upper()
    active = snap.get("active", {}) or {}
    active_unlimited = ", ".join(f"{k}={v}" for k, v in sorted(active.items())) if active else "none"
    slot_mode = str(settings.get_val("global_slot_mode", "fixed") or "fixed").lower()
    trade_sel = str(settings.get_val("trade_selection_policy", "best_score") or "best_score").lower()
    winrate = str(settings.get_val("winrate_profile", "balanced") or "balanced").lower()
    weekly_mode = str(settings.get_val("weekly_cap_mode", "adaptive") or "adaptive").lower()
    deploy_mode = str(settings.get_val("deployment_mode", "Guarded") or "Guarded")
    exposure_mode = str(settings.get_val("exposure_control_mode", "Strict") or "Strict")
    warnings = []
    if dbg_mode == "live_override" and active:
        warnings.append("Ã¢ÂšÂ Ã¯Â¸Â High-control state active")
    if slot_mode == "unlimited" and trade_sel == "replace_weakest":
        warnings.append("Ã¢ÂšÂ Ã¯Â¸Â Unlimited global slots with replacement enabled")
    if str(settings.get_val("size_mode", "blended") or "blended").lower() == "risk" and bool(settings.get_val("dynamic_sizing_enabled", True)):
        warnings.append("Ã¢ÂšÂ Ã¯Â¸Â Risk sizing + dynamic sizing active")
    return "\n".join([
        "Ã°ÂŸÂŽÂ›Ã¯Â¸Â *Control State*",
        DIV,
        f"Debug Override: `{dbg_mode}`",
        f"Active Unlimited: `{active_unlimited}`",
        f"Exposure Mode: `{exposure_mode}`",
        f"Global Slot Mode: `{slot_mode}`",
        f"Per-Engine Unlimited Caps: `enabled`",
        f"Trade Selection: `{trade_sel}`",
        f"Winrate Profile: `{winrate}`",
        f"Weekly Cap Mode: `{weekly_mode}`",
        f"Deployment Mode: `{deploy_mode}`",
        f"Control State: `ACTIVE`",
        "",
        *warnings,
    ])


def panel_api_health_text(settings) -> str:
    try:
        from category_engine import get_shared_okx as _get_okx
        rl = _get_okx().get_rate_limit_snapshot()
    except Exception:
        rl = {"recent_429_count": 0, "endpoint_pressure": [], "stale_warnings": [], "cache_status": {}}
    lines = [
        "Ã°ÂŸÂ“Â¡ *API Health*",
        DIV,
        f"Recent 429s: `{int(rl.get('recent_429_count', 0) or 0)}`",
    ]
    for row in (rl.get("endpoint_pressure", []) or [])[:5]:
        lines.append(
            f"`{row.get('endpoint','?')}` 429 `{int(row.get('429',0) or 0)}` / `{int(row.get('count',0) or 0)}`"
            f" pressure `{float(row.get('pressure_pct',0.0) or 0.0):.1f}%`"
        )
    if rl.get("stale_warnings"):
        lines.append("Warnings:")
        for item in rl["stale_warnings"]:
            lines.append(f"Ã¢Â€Â¢ {item}")
    cache = rl.get("cache_status", {}) or {}
    lines.append(f"Cache: taker=`{'warm' if cache.get('taker_volume_cached') else 'cold'}` long/short=`{'warm' if cache.get('long_short_ratio_cached') else 'cold'}`")
    lines.append(f"Account snapshot age: `{float(cache.get('account_snapshot_age_sec', 0.0) or 0.0):.1f}s`")
    return "\n".join(lines)


def panel_decision_trace_text(settings, limit: int = 12) -> str:
    try:
        import decision_trace as _dt
        recent = _dt.get_recent(limit=limit)
    except Exception:
        recent = []
    if not recent:
        return "Ã°ÂŸÂ§Â¾ *Decision Trace*\n\n_No recent traces recorded._"

    def fmt(row: dict) -> str:
        decision = str(row.get("final_decision", "") or "").lower()
        icon = {"admitted": "Ã¢ÂœÂ…", "soft accepted": "Ã°ÂŸÂŸÂ¡", "skipped": "Ã¢Â›Â”", "replaced_weakest_trade": "Ã¢Â™Â»Ã¯Â¸Â"}.get(decision, "Ã¢Â€Â¢")
        side = str(row.get("side", "") or "").upper()
        engine = str(row.get("engine", "") or "").upper()
        symbol = str(row.get("symbol", "") or "").upper()
        conf = float(row.get("confidence", 0.0) or 0.0)
        raw = float(row.get("raw_score", 0.0) or 0.0)
        slot = str(row.get("slot_state", "") or "")
        exp = str(row.get("exposure_state", "") or "")
        size = float(row.get("size_usdt", 0.0) or 0.0)
        reason = str(row.get("reasons", "") or "")
        return f"{icon} `{symbol}` {side} `{engine}` raw `{raw:.2f}` conf `{conf:.2f}` size `${size:.2f}`\n    slot `{slot}` exp `{exp}`\n    {reason[:120]}"

    candidates = [r for r in recent if str(r.get("final_decision", "")).lower() in {"skipped", "soft accepted", "admitted", "replaced_weakest_trade"}]
    accepted = [r for r in recent if str(r.get("final_decision", "")).lower() in {"admitted", "soft accepted", "replaced_weakest_trade"}]
    skipped = [r for r in recent if str(r.get("final_decision", "")).lower() == "skipped"]
    lines = ["Ã°ÂŸÂ§Â¾ *Decision Trace*", DIV]
    lines.append("Latest Candidates:")
    for row in candidates[-5:]:
        lines.append(fmt(row))
    lines.append("")
    lines.append("Latest Accepted:")
    for row in accepted[-5:]:
        lines.append(fmt(row))
    lines.append("")
    lines.append("Latest Skipped:")
    for row in skipped[-5:]:
        lines.append(fmt(row))
    return "\n".join(lines)


def panel_engine_diagnostics_text(settings, balance: float | None = None) -> str:
    try:
        from category_tracker import get_engine_stats, get_engine_recent_performance, ENGINE_NAMES, engine_display_name
    except Exception:
        return "Ã°ÂŸÂ§Â  *Engines*\n\n_No engine stats available._"
    try:
        from position_registry import get_all_details as _reg_details
        open_positions = _reg_details()
    except Exception:
        open_positions = {}
    try:
        from category_engine import get_silent_signal_diagnostics as _silent_diagnostics
        _funnel = _silent_diagnostics() or {}
        _funnel_by_engine = _funnel.get("by_engine", {}) or {}
    except Exception:
        _funnel_by_engine = {}
    try:
        from candidate_expectancy_telemetry import aggregate_engine_alpha as _aggregate_alpha, summarize as _research_summary

        _alpha = _aggregate_alpha(engine_names=ENGINE_NAMES)
        _research = _research_summary()
    except Exception:
        _alpha = {}
        _research = {}
    try:
        from production_research_funnel import summarize as _funnel_summary
        _production_funnel = _funnel_summary()
    except Exception:
        _production_funnel = {}
    scored = []
    for name in ENGINE_NAMES:
        s = get_engine_stats(name)
        perf = get_engine_recent_performance(name, 10, 30)
        role = settings.get_engine_role(name) if hasattr(settings, "get_engine_role") else str(settings.get_val(f"engine_{name.lower()}_role", "hunter"))
        mode = settings.get_engine_mode(name) if hasattr(settings, "get_engine_mode") else str(settings.get_val(f"engine_{name.lower()}_mode", "live"))
        trades = int(s.get("trade_count", 0) or 0)
        pnl = float(s.get("total_pnl", 0.0) or 0.0)
        wr = float(s.get("total_wr", 0.0) or 0.0)
        trades_list = s.get("trades", []) or []
        recent = trades_list[-20:]
        trend = "".join("W" if float(t.get("pnl_usdt", 0.0) or 0.0) >= 0 else "L" for t in recent) or "Ã¢Â€Â”"
        churn = (sum(1 for t in recent if float(t.get("pnl_usdt", 0.0) or 0.0) < 0) / max(len(recent), 1)) * 100.0
        if trades < 5:
            bucket = "noisy"
        elif wr >= 55.0 and pnl >= 0:
            bucket = "strong"
        elif wr >= 45.0:
            bucket = "mixed"
        elif pnl < 0:
            bucket = "weak"
        else:
            bucket = "noisy"
        strengths = []
        for pos in (open_positions or {}).values():
            if str(pos.get("engine", "") or "").upper() == name:
                strengths.append(float(pos.get("position_strength", 0.0) or 0.0))
            contrib = pos.get("role_contributions", {}) or {}
            if name in contrib:
                strengths.append(float((contrib.get(name) or {}).get("position_strength", 0.0) or 0.0))
        strength = max(strengths) if strengths else 0.0
        last10 = float(perf.get("last10_net", 0.0) or 0.0)
        last30 = float(perf.get("last30_net", 0.0) or 0.0)
        fee_pressure = float(perf.get("fee_to_profit_ratio", 0.0) or 0.0)
        dup_rate = float(perf.get("duplicate_entry_rate", 0.0) or 0.0)
        if mode == "shadow":
            promotion = "advising"
        elif mode == "probation" and last30 > 0:
            promotion = "protected"
        elif last10 < 0 and last30 < 0:
            promotion = "tighten"
        else:
            promotion = "eligible"
        engine_funnel = _funnel_by_engine.get(name, {}) or {}
        reason_counts = engine_funnel.get("reasons", {}) or {}
        dominant_blocker = max(reason_counts, key=lambda key: int(reason_counts.get(key, 0) or 0), default="none")
        observed = int(sum(int(value or 0) for value in reason_counts.values()))
        alpha = dict(_alpha.get(name, {}) or {})
        candidate_count = int(alpha.get("candidates", 0) or 0)
        gross_positive = alpha.get("positive_gross_pct")
        cf_winners = int(alpha.get("counterfactual_winners", 0) or 0)
        false_negative = alpha.get("false_negative_rate")
        alpha_text = (
            f"alpha `{candidate_count}c` gross+ `{'n/a' if gross_positive is None else f'{float(gross_positive):.0f}%'}` "
            f"cfW `{cf_winners}` FN `{'n/a' if false_negative is None else f'{float(false_negative) * 100.0:.0f}%'}`"
        )
        scored.append((
            bucket,
            f"`{name}` `{mode}/{role}` {trades}t `${pnl:+.2f}` WR `{wr:.0f}%` "
            f"obs `{observed}` block `{dominant_blocker}` status `{promotion}`\n  {alpha_text}",
        ))
    order = {"strong": 0, "mixed": 1, "weak": 2, "noisy": 3}
    scored.sort(key=lambda x: (order.get(x[0], 9), x[1]))
    lines = [
        "Ã°ÂŸÂ§Â  *Engines*",
        DIV,
        "Production Research:",
        f"Opportunities `{int(_research.get('unique_opportunities', 0) or 0)}` | revisions `{int(_research.get('candidate_revisions', 0) or 0)}` | resolved `{int(_research.get('resolved_count', 0) or 0)}`",
        f"Gross+ `{int(_research.get('positive_gross_edge_count', 0) or 0)}` | Net+ `{int(_research.get('positive_net_edge_count', 0) or 0)}` | correct reject `{int(_research.get('correct_rejection_count', 0) or 0)}` | false negative `{int(_research.get('false_negative_count', 0) or 0)}`",
        f"Evidence `{str(_research.get('evidence_maturity', 'INSUFFICIENT') or 'INSUFFICIENT')}`",
        "",
        "Research Funnel:",
        f"Scans `{int(_production_funnel.get('scans', 0) or 0)}` | contexts `{int((_production_funnel.get('counts', {}) or {}).get('context_offered', 0) or 0)}` | evaluations `{int((_production_funnel.get('counts', {}) or {}).get('engine_evaluation', 0) or 0)}`",
        f"HOLDs `{int((_production_funnel.get('counts', {}) or {}).get('raw_hold', 0) or 0)}` | signals `{int((_production_funnel.get('counts', {}) or {}).get('raw_signal', 0) or 0)}` | pre-candidates `{int((_production_funnel.get('counts', {}) or {}).get('pre_candidate', 0) or 0)}` | candidates `{int((_production_funnel.get('counts', {}) or {}).get('candidate', 0) or 0)}`",
        "",
    ]
    for bucket in ("strong", "mixed", "weak", "noisy"):
        subset = [text for b, text in scored if b == bucket]
        if subset:
            lines.append(f"{bucket.title()}:")
            lines.extend(subset)
            lines.append("")
    return "\n".join(lines).rstrip()


def panel_diagnostics_text(settings, balance: float | None = None) -> str:
    telemetry = "ON" if bool(settings.get_val("v6_telemetry_enabled", True)) else "OFF"
    block_detail = "ON" if bool(settings.get_val("block_diagnostics_enabled", True)) else "OFF"
    invalidation_telemetry = "ON" if bool(settings.get_val("profitability_telemetry_enabled", True)) else "OFF"
    return "\n".join([
        "*Diagnostics*",
        DIV,
        "Read-only runtime inspection, decision traces, health, and state visibility.",
        f"Telemetry: `{telemetry}`",
        f"Block Detail: `{block_detail}`",
        f"Invalidation Telemetry: `{invalidation_telemetry}`",
        "Control State is available here as inspection only.",
    ])


def panel_control_state_text(settings, balance: float | None = None) -> str:
    try:
        import settings_store as _ss
        snap = _ss.get_debug_override_snapshot() or {}
    except Exception:
        snap = {"mode": "off", "active": {}}
    dbg_mode = str(snap.get("mode", "off") or "off").upper()
    active = snap.get("active", {}) or {}
    active_unlimited = ", ".join(f"{k}={v}" for k, v in sorted(active.items())) if active else "none"
    slot_mode = str(settings.get_val("global_slot_mode", "fixed") or "fixed").lower()
    trade_sel = str(settings.get_val("trade_selection_policy", "best_score") or "best_score").lower()
    winrate = str(settings.get_val("winrate_profile", "balanced") or "balanced").lower()
    weekly_mode = str(settings.get_val("weekly_cap_mode", "adaptive") or "adaptive").lower()
    deploy_mode = str(settings.get_val("deployment_mode", "Guarded") or "Guarded")
    exposure_mode = str(settings.get_val("exposure_control_mode", "Strict") or "Strict")
    warnings = []
    if dbg_mode == "LIVE_OVERRIDE" and active:
        warnings.append("Ã¢ÂšÂ Ã¯Â¸Â High-control state active")
    if slot_mode == "unlimited" and trade_sel == "replace_weakest":
        warnings.append("Ã¢ÂšÂ Ã¯Â¸Â Unlimited global slots with replacement enabled")
    if str(settings.get_val("size_mode", "blended") or "blended").lower() == "risk" and bool(settings.get_val("dynamic_sizing_enabled", True)):
        warnings.append("Ã¢ÂšÂ Ã¯Â¸Â Risk sizing + dynamic sizing active")
    top_block = warnings[0].replace("Ã¢ÂšÂ Ã¯Â¸Â ", "") if warnings else "none"
    return "\n".join([
        "Ã°ÂŸÂŽÂ›Ã¯Â¸Â *Control State*",
        DIV,
        f"Debug Overrides: `{dbg_mode}` (`{active_unlimited}`)",
        f"Top Block: `{top_block}`",
        f"Debug Override: `{dbg_mode}`",
        f"Active Unlimited: `{active_unlimited}`",
        f"Exposure Mode: `{exposure_mode}`",
        f"Global Slot Mode: `{slot_mode}`",
        f"Per-Engine Unlimited Caps: `enabled`",
        f"Trade Selection: `{trade_sel}`",
        f"Winrate Profile: `{winrate}`",
        f"Weekly Cap Mode: `{weekly_mode}`",
        f"Deployment Mode: `{deploy_mode}`",
        f"Control State: `ACTIVE`",
        "",
        *warnings,
    ])


def panel_diagnostics_text(settings, balance: float | None = None) -> str:
    return "\n".join([
        "Ã°ÂŸÂ§Âª *Diagnostics*",
        DIV,
        panel_control_state_text(settings, balance=balance).split("\n", 1)[1],
    ])


# AI-free runtime overrides. These supersede older menu builders below.

def panel_control_state_text(settings, balance: float | None = None) -> str:
    try:
        import settings_store as _ss
        snap = _ss.get_debug_override_snapshot() or {}
    except Exception:
        snap = {"mode": "off", "active": {}}
    dbg_mode = str(snap.get("mode", "off") or "off").upper()
    active = snap.get("active", {}) or {}
    active_unlimited = ", ".join(f"{k}={v}" for k, v in sorted(active.items())) if active else "none"
    slot_mode = str(settings.get_val("global_slot_mode", "fixed") or "fixed").lower()
    trade_sel = str(settings.get_val("trade_selection_policy", "best_score") or "best_score").lower()
    winrate = str(settings.get_val("winrate_profile", "balanced") or "balanced").lower()
    weekly_mode = str(settings.get_val("weekly_cap_mode", "adaptive") or "adaptive").lower()
    deploy_mode = str(settings.get_val("deployment_mode", "Guarded") or "Guarded")
    exposure_mode = str(settings.get_val("exposure_control_mode", "Strict") or "Strict")
    warnings = []
    if dbg_mode == "LIVE_OVERRIDE" and active:
        warnings.append("? High-control state active")
    if slot_mode == "unlimited" and trade_sel == "replace_weakest":
        warnings.append("? Unlimited global slots with replacement enabled")
    if str(settings.get_val("size_mode", "blended") or "blended").lower() == "risk" and bool(settings.get_val("dynamic_sizing_enabled", True)):
        warnings.append("? Risk sizing + dynamic sizing active")
    top_block = warnings[0].replace("? ", "") if warnings else "none"
    return "\n".join([
        "? *Control State*",
        DIV,
        f"Debug Overrides: `{dbg_mode}` (`{active_unlimited}`)",
        f"Top Block: `{top_block}`",
        f"Debug Override: `{dbg_mode}`",
        f"Active Unlimited: `{active_unlimited}`",
        f"Exposure Mode: `{exposure_mode}`",
        f"Global Slot Mode: `{slot_mode}`",
        f"Trade Selection: `{trade_sel}`",
        f"Winrate Profile: `{winrate}`",
        f"Weekly Cap Mode: `{weekly_mode}`",
        f"Deployment Mode: `{deploy_mode}`",
        "",
        *warnings,
    ])


def kb_home(running: bool = False) -> InlineKeyboardMarkup:
    """Compact production-only operator terminal."""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📊 Live Status", callback_data="terminal:status"),
         InlineKeyboardButton("💰 Account", callback_data="terminal:account")],
        [InlineKeyboardButton("📈 Positions", callback_data="terminal:positions"),
         InlineKeyboardButton("🎯 Opportunities", callback_data="terminal:opportunities")],
        [InlineKeyboardButton("🧠 Engines", callback_data="terminal:engines"),
         InlineKeyboardButton("⚡ Execution", callback_data="terminal:execution")],
        [InlineKeyboardButton("🛡 Risk & Sizing", callback_data="terminal:risk"),
         InlineKeyboardButton("🎯 Exits", callback_data="terminal:exits")],
        [InlineKeyboardButton("💧 Liquidity", callback_data="terminal:liquidity"),
         InlineKeyboardButton("📚 Performance", callback_data="terminal:performance")],
        [InlineKeyboardButton("🔬 Research", callback_data="terminal:research"),
         InlineKeyboardButton("⚙️ System", callback_data="terminal:system")],
        [InlineKeyboardButton("🔎 Why No Trade?", callback_data="terminal:why_no_trade"),
         InlineKeyboardButton("🧾 Recent Events", callback_data="terminal:recent_events")],
        [InlineKeyboardButton("🚨 Emergency", callback_data="terminal:emergency")],
    ])


def kb_terminal_back(*, emergency: bool = False) -> InlineKeyboardMarkup:
    rows = []
    if emergency:
        rows.extend([
            [InlineKeyboardButton("🚨 DISARM NEW ENTRIES", callback_data="live:disarm")],
            [InlineKeyboardButton("🛑 HALT", callback_data="live:halt_confirm")],
        ])
    rows.append([InlineKeyboardButton("🏠 Home", callback_data="main_menu")])
    return InlineKeyboardMarkup(rows)


def kb_settings_main(settings) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🏠 Dashboard", callback_data="s:dashboard"),
         InlineKeyboardButton("📐 Strategy", callback_data="s:strategy")],
        [InlineKeyboardButton("🤝 Participation", callback_data="s:participation"),
         InlineKeyboardButton("📡 Execution", callback_data="s:execution")],
        [InlineKeyboardButton("🧪 Filters", callback_data="s:filters"),
         InlineKeyboardButton("🧵 Slots", callback_data="s:slots")],
        [InlineKeyboardButton("🛡 Risk", callback_data="s:risk_cfg"),
         InlineKeyboardButton("⚙️ Engines", callback_data="s:engines")],
        [InlineKeyboardButton("🧾 Diagnostics", callback_data="s:diagnostics"),
         InlineKeyboardButton("🔧 Advanced", callback_data="s:advanced")],
        [InlineKeyboardButton("🖥 System", callback_data="s:system"),
         InlineKeyboardButton("♻️ Reset Defaults", callback_data="stg_reset_confirm")],
        [InlineKeyboardButton("🏠 Home", callback_data="main_menu")],
    ])


def kb_settings_category(settings) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("💰 Capital", callback_data="set:cat:capital"),
         InlineKeyboardButton("🖥 System", callback_data="set:cat:system")],
        [InlineKeyboardButton("🧾 Diagnostics", callback_data="set:cat:diagnostics"),
         InlineKeyboardButton("📡 Execution", callback_data="set:cat:execution")],
        [InlineKeyboardButton("🛡 Risk", callback_data="set:cat:risk"),
         InlineKeyboardButton("🤝 Participation", callback_data="set:cat:participation")],
        [InlineKeyboardButton("🛠 Settings", callback_data="settings_menu")],
    ])


def panel_settings_ai_text(settings) -> str:
    return "\n".join([
        "? *Settings*",
        DIV,
        "AI support has been removed from OxyPulse.",
        "Use the other settings rooms for trading controls.",
    ])


def kb_settings_ai(settings) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("? Settings", callback_data="settings_menu")],
    ])


def panel_settings_cat_ai_text(settings) -> str:
    return "\n".join([
        "? *Settings Â· AI*",
        DIV,
        "AI support has been removed.",
    ])


def kb_settings_cat_ai(settings) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("? Settings", callback_data="settings_menu")],
    ])



def panel_settings_help_text() -> str:
    return "\n".join([
        "Help",
        DIV,
        "Headroom = remaining safe room before a new trade can open.",
        "Preflight = exchange admission check before order placement.",
    ])


def kb_settings_advanced(settings, *args, **kwargs) -> InlineKeyboardMarkup:
    return kb_settings_diagnostics(settings)


def kb_settings_help() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Modes", callback_data="s:modes"), InlineKeyboardButton("Engines", callback_data="s:engines")],
        [InlineKeyboardButton("? Settings", callback_data="settings_menu")],
    ])


_panel_settings_dashboard_text_base = panel_settings_dashboard_text


def panel_settings_dashboard_text(
    settings,
    *,
    balance: float | None = None,
    exposure=None,
    running: bool = False,
    status_label: str = "STOPPED",
    regime: str = "-",
    session: str = "-",
    sync_status: str = "Standby",
    scanner_status: str = "Idle",
    open_positions: int = 0,
    live_snapshot: dict | None = None,
    ) -> str:
    text = _panel_settings_dashboard_text_base(
        settings,
        balance=balance,
        exposure=exposure,
        running=running,
        status_label=status_label,
        regime=regime,
        session=session,
        sync_status=sync_status,
        scanner_status=scanner_status,
        open_positions=open_positions,
    )
    if live_snapshot is not None:
        open_pnl = live_snapshot.get("open_pnl_usdt")
        source = live_snapshot.get("source_label") or _truth_source_label(live_snapshot)
        if open_pnl is None:
            text += f"\nOpen PnL: `—` ({source}, {_truth_age(live_snapshot)})"
        else:
            text += f"\nOpen PnL: {_fmt_pnl(float(open_pnl))} ({source}, {_truth_age(live_snapshot)})"
    return text.replace("Ã‚Â·", "Â·").replace("AI Gatekeeper", "Review Gate").replace("AI Gate", "Review Gate")


def panel_fg_ai_gate_text(settings) -> str:
    on = bool(settings.get_val("v6_ai_veto_enabled", False))
    fallback = str(settings.get_val("v6_ai_gate_fallback", "bypass") or "bypass").upper()
    return "\n".join([
        "ðŸ”Ž *Review Gate*",
        DIV,
        f"Enabled: `{ 'ON' if on else 'OFF' }` Â· Fallback: `{fallback}`",
        "",
        "_If the review gate is unavailable, trading continues with the configured fallback._",
    ])


def kb_fg_ai_gate(settings) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Return", callback_data="diag:control_state")],
    ])


def kb_settings_cat_ai(settings) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("â† Settings", callback_data="settings_menu")],
    ])


def panel_room_size_mode(settings) -> str:
    cur = str(settings.get_val("size_mode", "blended") or "blended").strip().lower()
    cur = cur if cur in {"notional", "balance", "risk", "blended"} else "blended"
    labels = {
        "notional": "NOTIONAL",
        "balance": "BALANCE",
        "risk": "RISK",
        "blended": "BLENDED",
    }
    return _room_header(
        "Size Mode",
        "Controls how base trade size is derived for NEW entries.",
        labels.get(cur, "BLENDED"),
    )


def kb_room_size_mode(settings) -> InlineKeyboardMarkup:
    cur = str(settings.get_val("size_mode", "blended") or "blended").strip().lower()
    cur = cur if cur in {"notional", "balance", "risk", "blended"} else "blended"

    def _btn(label: str, value: str) -> InlineKeyboardButton:
        suffix = " *" if cur == value else ""
        return InlineKeyboardButton(label + suffix, callback_data=f"room:size_mode:{value}")

    return InlineKeyboardMarkup([
        [_btn("NOTIONAL", "notional"), _btn("BALANCE", "balance")],
        [_btn("RISK", "risk"), _btn("BLENDED", "blended")],
        [InlineKeyboardButton("Back to Size", callback_data="set:fg:size")],
        [InlineKeyboardButton("Back to Capital", callback_data="set:cat:capital")],
        [InlineKeyboardButton("Back to Settings", callback_data="settings_menu")],
    ])


def panel_debug_overrides_text(settings) -> str:
    try:
        import settings_store as _ss
        snap = _ss.get_debug_override_snapshot() or {}
    except Exception:
        snap = {"mode": "off", "levels": {}, "active": {}}
    mode = str(snap.get("mode", "off") or "off").upper()
    levels = snap.get("levels", {}) or {}
    active = snap.get("active", {}) or {}
    lines = [
        "*Debug Overrides*",
        DIV,
        "Operator diagnostic overrides for NEW admission checks only.",
        "",
        f"Mode: `{mode}`",
        f"Active: `{len(active)}`",
        "",
        "*Families:*",
    ]
    families = (
        "global_cap",
        "engine_cap",
        "weekly_cap",
        "exposure_cap",
        "correlation_cap",
        "same_direction_cap",
        "portfolio_direction_cap",
        "lane_budget",
    )
    for fam in families:
        lines.append(f"`{fam}`: `{str(levels.get(fam, 'normal')).upper()}`")
    return "\n".join(lines)


def kb_debug_overrides(settings) -> InlineKeyboardMarkup:
    try:
        import settings_store as _ss
        snap = _ss.get_debug_override_snapshot() or {}
    except Exception:
        snap = {"mode": "off", "levels": {}}
    mode = str(snap.get("mode", "off") or "off").lower()
    levels = snap.get("levels", {}) or {}

    def _mode(label: str, value: str) -> InlineKeyboardButton:
        suffix = " *" if mode == value else ""
        return InlineKeyboardButton(label + suffix, callback_data=f"dbg:mode:{value}")

    rows: list[list[InlineKeyboardButton]] = [
        [_mode("OFF", "off"), _mode("PROBE", "probe"), _mode("LIVE", "live_override")],
    ]
    families = (
        "global_cap",
        "engine_cap",
        "weekly_cap",
        "exposure_cap",
        "correlation_cap",
        "same_direction_cap",
        "portfolio_direction_cap",
        "lane_budget",
    )
    for fam in families:
        level = str(levels.get(fam, "normal") or "normal").upper()
        rows.append([InlineKeyboardButton(f"{fam} [{level}]", callback_data=f"dbg:cycle:{fam}")])
    rows.append([InlineKeyboardButton("Reset Overrides", callback_data="dbg:reset")])
    rows.append([InlineKeyboardButton("Back to Diagnostics", callback_data="s:diagnostics")])
    return InlineKeyboardMarkup(rows)


def panel_diag_risk_text(settings, balance: float | None = None) -> str:
    bal = max(float(balance or 0.0), 0.0)
    try:
        from exposure_tracker import get_exposure as _get_exposure
        exp = _get_exposure(bal)
        exposure_line = (
            f"Long `${float(getattr(exp, 'long_usdt', 0.0) or 0.0):.2f}` "
            f"Short `${float(getattr(exp, 'short_usdt', 0.0) or 0.0):.2f}` "
            f"Net `{float(getattr(exp, 'net_exposure_pct', 0.0) or 0.0):.1f}%`"
        )
    except Exception as exc:
        exposure_line = f"Exposure unavailable: `{type(exc).__name__}`"
    try:
        import performance_tracker as _wk
        hit, pct = _wk.is_weekly_cap_hit(bal)
        weekly_line = f"Weekly Cap: `{'HIT' if hit else 'OK'}` `{float(pct or 0.0):.1f}%`"
    except Exception as exc:
        weekly_line = f"Weekly Cap unavailable: `{type(exc).__name__}`"
    return "\n".join([
        "*Risk Diagnostics*",
        DIV,
        exposure_line,
        weekly_line,
        f"Risk Mode: `{settings.get_val('exposure_control_mode', 'Strict')}`",
        f"Weekly Mode: `{settings.get_val('weekly_cap_mode', 'adaptive')}`",
    ])


def panel_micro_sizing_text(settings, balance: float | None = None) -> str:
    snap = _strategy_runtime_snapshot(settings, balance=balance)
    try:
        import micro_sizing as _micro_sizing
        last = _micro_sizing.get_last_sizing_diagnostics()
        diag = _micro_sizing.diagnostics_snapshot(
            balance_usdt=float(snap.get("balance", 0.0) or 0.0),
            settings=settings,
            leverage=int(snap.get("live_leverage", settings.get_val("leverage_default", 10)) or settings.get_val("leverage_default", 10)),
            exchange_floor_usdt=float(last.get("exchange_symbol_floor_usdt", 0.0) or 0.0),
            final_size_usdt=float(last.get("final_executable_size_usdt", snap.get("projected_live_size", 0.0)) or 0.0),
            block_reason=str(last.get("block_reason", "") or ""),
            reductions_stacked=bool(last.get("reductions_stacked", False)),
            effective_max_positions=int(snap.get("deployment_max_positions", 0) or 0),
        )
    except Exception:
        last = {}
        diag = {
            "micro_sizing_active": False,
            "effective_account_floor_usdt": float(snap.get("exec_floor", 0.0) or 0.0),
            "exchange_symbol_floor_usdt": 0.0,
            "effective_margin_buffer_usdt": float(snap.get("effective_margin_buffer_usdt", 0.0) or 0.0),
            "final_executable_size_usdt": float(snap.get("projected_live_size", 0.0) or 0.0),
            "final_margin_requirement_usdt": float(snap.get("margin_used", 0.0) or 0.0),
            "block_reason": "",
            "reductions_stacked": False,
            "effective_max_positions": int(snap.get("deployment_max_positions", 0) or 0),
        }
    return "\n".join([
        "*Micro Sizing Diagnostics*",
        DIV,
        f"Micro sizing mode: `{ 'ACTIVE' if diag.get('micro_sizing_active') else 'OFF' }`",
        f"Effective Account Floor: `${float(diag.get('effective_account_floor_usdt', 0.0) or 0.0):.2f}`",
        f"Exchange/Symbol Floor: `${float(diag.get('exchange_symbol_floor_usdt', 0.0) or 0.0):.2f}`",
        f"Effective Margin Buffer: `${float(diag.get('effective_margin_buffer_usdt', 0.0) or 0.0):.2f}`",
        f"Final Executable Size: `${float(diag.get('final_executable_size_usdt', 0.0) or 0.0):.2f}`",
        f"Final Margin Requirement: `${float(diag.get('final_margin_requirement_usdt', 0.0) or 0.0):.4f}`",
        f"Effective Max Positions: `{diag.get('effective_max_positions', snap.get('deployment_max_positions', 0))}`",
        f"Reductions Stacked: `{ 'YES' if diag.get('reductions_stacked') else 'NO' }`",
        f"Latest Block Reason: `{str(diag.get('block_reason') or last.get('reason') or 'none')[:160]}`",
        "",
        f"Projected Live Size: `${float(snap.get('projected_live_size', 0.0) or 0.0):.2f}`",
        f"Deployable Notional: `${float(snap.get('deployment_total_deployable_usdt', 0.0) or 0.0):.2f}`",
        f"Deployment Slot Target: `${float(snap.get('deployment_per_position_target_usdt', 0.0) or 0.0):.2f}`",
        HOT_SCOPE_NOTE,
    ])


def panel_diag_strategy_text(settings, balance: float | None = None) -> str:
    try:
        from category_tracker import ENGINE_NAMES
        role_counts: dict[str, int] = {}
        mode_counts: dict[str, int] = {}
        for name in ENGINE_NAMES:
            role = settings.get_engine_role(name) if hasattr(settings, "get_engine_role") else str(settings.get_val(f"engine_{name.lower()}_role", "hunter"))
            mode = settings.get_engine_mode(name) if hasattr(settings, "get_engine_mode") else str(settings.get_val(f"engine_{name.lower()}_mode", "live"))
            role_counts[role] = role_counts.get(role, 0) + 1
            mode_counts[mode] = mode_counts.get(mode, 0) + 1
    except Exception:
        role_counts = {}
        mode_counts = {}
    role_line = ", ".join(f"{k}={v}" for k, v in sorted(role_counts.items())) or "none"
    mode_line = ", ".join(f"{k}={v}" for k, v in sorted(mode_counts.items())) or "none"
    return "\n".join([
        "*Strategy Diagnostics*",
        DIV,
        f"Engine Modes: `{mode_line}`",
        f"Engine Roles: `{role_line}`",
        f"Merge Window: `{int(settings.get_val('duplicate_merge_window_sec', 90) or 90)}s`",
        f"Selection: `{settings.get_val('trade_selection_policy', 'best_score')}`",
        f"Slot Mode: `{settings.get_val('global_slot_mode', 'fixed')}`",
    ])


def panel_no_trades_text(settings, balance: float | None = None) -> str:
    try:
        from trading_engine import engine as _engine
        snap = _engine.trade_flow_snapshot()
    except Exception as exc:
        snap = {"last_error": f"snapshot unavailable: {type(exc).__name__}"}
    try:
        from category_tracker import ENGINE_NAMES
        active = 0
        roles: dict[str, int] = {}
        modes: dict[str, int] = {}
        for name in ENGINE_NAMES:
            mode = settings.get_engine_mode(name) if hasattr(settings, "get_engine_mode") else str(settings.get_val(f"engine_{name.lower()}_mode", "live"))
            role = settings.get_engine_role(name) if hasattr(settings, "get_engine_role") else str(settings.get_val(f"engine_{name.lower()}_role", "hunter"))
            modes[mode] = modes.get(mode, 0) + 1
            roles[role] = roles.get(role, 0) + 1
            if mode in {"live", "probation"}:
                active += 1
    except Exception:
        active = 0
        roles = {}
        modes = {}
    blockers = snap.get("top_blockers", {}) or {}
    decisions = snap.get("decision_summary", {}) or {}
    blocker_line = ", ".join(f"{k}={v}" for k, v in list(blockers.items())[:5]) or "none recorded"
    decision_line = ", ".join(f"{k}={v}" for k, v in list(decisions.items())[:5]) or "none recorded"
    timeouts = ", ".join(snap.get("engine_timeouts", []) or []) or "none"
    errors = "; ".join(snap.get("engine_errors", []) or []) or str(snap.get("last_error", "") or "none")
    role_line = ", ".join(f"{k}={v}" for k, v in sorted(roles.items())) or "unknown"
    mode_line = ", ".join(f"{k}={v}" for k, v in sorted(modes.items())) or "unknown"
    try:
        import performance_tracker as _wk
        hit, pct = _wk.is_weekly_cap_hit(float(balance or 0.0))
        weekly_line = f"{'HIT' if hit else 'OK'} {float(pct or 0.0):.1f}%"
    except Exception:
        weekly_line = "unknown"
    return "\n".join([
        "*Why No Trades?*",
        DIV,
        f"Last Scan Start: `{snap.get('started_at', '') or 'n/a'}`",
        f"Last Scan Finish: `{snap.get('finished_at', '') or 'not finished'}`",
        f"Scan Duration: `{float(snap.get('duration_sec', 0.0) or 0.0):.1f}s`",
        f"Candidates Scanned: `{int(snap.get('universe_count', 0) or 0)}`",
        f"Active Engines: `{active}` runtime `{int(snap.get('active_engine_count', 0) or 0)}`",
        f"Open Positions: `{int(snap.get('open_positions', 0) or 0)}`",
        f"Weekly Cap: `{weekly_line}`",
        f"Top Blockers: `{blocker_line}`",
        f"Decision Trace: `{decision_line}`",
        f"Engine Timeouts: `{timeouts}`",
        f"Last Error: `{errors[:140]}`",
        "",
        f"Modes: `{mode_line}`",
        f"Roles: `{role_line}`",
    ])


# Telegram control-plane integrity overrides.  These builders intentionally use
# ASCII labels/text so the late mojibake normalizer has nothing to repair.

def _setting_value(settings, key: str, default=None):
    try:
        return settings.get_val(key, default)
    except Exception:
        return default


def _setting_state(settings, key: str, default=False) -> str:
    return "ON" if bool(_setting_value(settings, key, default)) else "OFF"


def _setting_num(settings, key: str, default=0.0, decimals: int = 2) -> str:
    try:
        return f"{float(_setting_value(settings, key, default)):.{decimals}f}"
    except Exception:
        return str(_setting_value(settings, key, default))


def _novo_state(settings, key: str, default=False) -> str:
    return "ON" if bool(_setting_value(settings, key, default)) else "OFF"


def _novo_master_enabled(settings) -> bool:
    return bool(_setting_value(settings, "novo_intelligence_enabled", False))


def _novo_master_line(settings) -> str:
    return "Novo Intelligence: `ENABLED`" if _novo_master_enabled(settings) else "Novo Intelligence: `DISABLED`"


def _novo_collecting_line(settings) -> str:
    return "Status: `COLLECTING / NO DATA YET`" if _novo_master_enabled(settings) else "Status: `DISABLED`"


def _latest_novo_trace(limit=50) -> tuple[dict | None, dict | None]:
    """Return the newest decision trace carrying compact Novo metadata."""
    try:
        import decision_trace as _dt
        recent = _dt.get_recent(limit=limit)
        for trace in reversed(list(recent or [])):
            if not isinstance(trace, dict):
                continue
            extra = trace.get("extra", {})
            if not isinstance(extra, dict):
                continue
            novo = extra.get("novo")
            if isinstance(novo, dict):
                return trace, novo
            conf_trace = extra.get("confidence_stage_trace")
            if isinstance(conf_trace, dict) and isinstance(conf_trace.get("novo"), dict):
                return trace, conf_trace.get("novo")
        return None, None
    except Exception:
        return None, None


def _latest_novo_enrichment_trace(limit=50) -> tuple[dict | None, dict | None]:
    """Return the newest decision trace carrying compact Novo enrichment metadata."""
    try:
        import decision_trace as _dt
        recent = _dt.get_recent(limit=limit)
        for trace in reversed(list(recent or [])):
            if not isinstance(trace, dict):
                continue
            extra = trace.get("extra", {})
            if not isinstance(extra, dict):
                continue
            enrichment = extra.get("novo_enrichment")
            if isinstance(enrichment, dict):
                return trace, enrichment
            conf_trace = extra.get("confidence_stage_trace")
            if isinstance(conf_trace, dict) and isinstance(conf_trace.get("novo_enrichment"), dict):
                return trace, conf_trace.get("novo_enrichment")
        return None, None
    except Exception:
        return None, None


def _novo_trace_header(trace: dict | None) -> list[str]:
    if not isinstance(trace, dict):
        return []
    symbol = str(trace.get("symbol", "") or "").upper()
    side = str(trace.get("side", "") or "").upper()
    engine = str(trace.get("engine", "") or "").upper()
    ts = str(trace.get("ts", "") or "")
    lines = []
    if symbol or side or engine:
        lines.append(f"Latest: `{symbol or '?'} {side or '?'} [{engine or '?'}]`")
    if ts:
        lines.append(f"Trace Time: `{ts}`")
    return lines


def _novo_module_status(novo: dict | None, module: str) -> str:
    try:
        modules = novo.get("modules", {}) if isinstance(novo, dict) else {}
        value = modules.get(module, "UNKNOWN") if isinstance(modules, dict) else "UNKNOWN"
        return str(value or "UNKNOWN")
    except Exception:
        return "UNKNOWN"


def _novo_module_summary(novo: dict | None) -> str:
    modules = novo.get("modules", {}) if isinstance(novo, dict) else {}
    if not isinstance(modules, dict) or not modules:
        return "`COLLECTING`"
    ordered = ("market_dna", "market_structure", "bias_4h", "crt", "liquidity_sweep", "session_model")
    parts = [f"{name}:{str(modules.get(name, 'UNKNOWN') or 'UNKNOWN')}" for name in ordered]
    return "`" + " | ".join(parts) + "`"


def _novo_multiplier(value, default: float = 1.0) -> str:
    try:
        return f"x{float(value if value is not None else default):.2f}"
    except Exception:
        return f"x{float(default):.2f}"


def _novo_enrichment_lines() -> list[str]:
    _, enrichment = _latest_novo_enrichment_trace()
    if not isinstance(enrichment, dict):
        return ["Dry-Run Enrichment: `COLLECTING / NO ENRICHMENT TRACE YET`"]
    try:
        suggestions = enrichment.get("suggestions", {})
        if not isinstance(suggestions, dict):
            suggestions = {}
        return [
            f"Dry-Run Enrichment: `{str(enrichment.get('status', 'COLLECTING') or 'COLLECTING')}`",
            f"Band: `{str(enrichment.get('band', 'COLLECTING') or 'COLLECTING')}`",
            f"TP2 Multiplier: `{_novo_multiplier(suggestions.get('tp2_multiplier', 1.0))}`",
            f"Trailing Activation: `{_novo_multiplier(suggestions.get('trailing_activation_multiplier', 1.0))}`",
            f"BE Delay: `{_novo_multiplier(suggestions.get('break_even_delay_multiplier', 1.0))}`",
            f"Profit Lock: `{_novo_multiplier(suggestions.get('profit_lock_multiplier', 1.0))}`",
            f"Partial Min Notional: `{_novo_multiplier(suggestions.get('partial_min_notional_multiplier', 1.0))}`",
            f"Execution Split: `{str(suggestions.get('execution_split_preference', 'UNCHANGED') or 'UNCHANGED')}`",
            f"Size Multiplier: `{_novo_multiplier(suggestions.get('size_multiplier', 1.0))}`",
        ]
    except Exception:
        return ["Dry-Run Enrichment: `COLLECTING / NO ENRICHMENT TRACE YET`"]


def render_novo_quality_panel(settings=None) -> str:
    """Read-only Novo quality diagnostics panel."""
    if not _novo_master_enabled(settings):
        return "\n".join([
            "*Novo Quality*",
            "------------------",
            _novo_master_line(settings),
            f"Advisory Only: `{_novo_state(settings, 'novo_trade_quality_advisory_only', True)}`",
            f"No Hard Blocks: `{_novo_state(settings, 'novo_no_hard_blocks', True)}`",
            f"Trade Quality Module: `{_novo_state(settings, 'novo_trade_quality_enabled', True)}`",
            "Status: `DISABLED`",
            "Quality Score: `COLLECTING`",
        ])
    trace, novo = _latest_novo_trace()
    if not novo:
        return "\n".join([
            "*Novo Quality*",
            "------------------",
            _novo_master_line(settings),
            f"Advisory Only: `{_novo_state(settings, 'novo_trade_quality_advisory_only', True)}`",
            f"No Hard Blocks: `{_novo_state(settings, 'novo_no_hard_blocks', True)}`",
            f"Trade Quality Module: `{_novo_state(settings, 'novo_trade_quality_enabled', True)}`",
            "Status: `COLLECTING / NO NOVO TRACE YET`",
            "Quality Score: `COLLECTING`",
            *_novo_enrichment_lines(),
        ])
    lines = [
        "*Novo Quality*",
        "------------------",
        _novo_master_line(settings),
        *_novo_trace_header(trace),
        f"Advisory Only: `{bool(novo.get('advisory_only', True))}`",
        f"No Hard Blocks: `{_novo_state(settings, 'novo_no_hard_blocks', True)}`",
        f"Quality Score: `{novo.get('quality_score', 'COLLECTING')}`",
        f"Quality Tier: `{novo.get('quality_tier', 'COLLECTING')}`",
        f"Modules: {_novo_module_summary(novo)}",
        *_novo_enrichment_lines(),
    ]
    return "\n".join(lines)


def _render_novo_module_panel(settings, *, title: str, flag_key: str, module_key: str, label: str, empty_label: str) -> str:
    if not _novo_master_enabled(settings):
        return "\n".join([
            f"*{title}*",
            "------------------",
            _novo_master_line(settings),
            f"{label} Module: `{_novo_state(settings, flag_key, True)}`",
            "Status: `DISABLED`",
            f"{label}: `{empty_label}`",
        ])
    trace, novo = _latest_novo_trace()
    if not novo:
        return "\n".join([
            f"*{title}*",
            "------------------",
            _novo_master_line(settings),
            f"{label} Module: `{_novo_state(settings, flag_key, True)}`",
            "Status: `COLLECTING / NO NOVO TRACE YET`",
            f"{label}: `{empty_label}`",
        ])
    lines = [
        f"*{title}*",
        "------------------",
        _novo_master_line(settings),
        *_novo_trace_header(trace),
        f"{label} Module: `{_novo_state(settings, flag_key, True)}`",
        f"Latest {label}: `{_novo_module_status(novo, module_key)}`",
        f"Quality: `{novo.get('quality_score', 'COLLECTING')} / {novo.get('quality_tier', 'COLLECTING')}`",
    ]
    return "\n".join(lines)


def _render_novo_quality_panel_legacy(settings=None) -> str:
    """Legacy collecting-only Novo quality diagnostics panel."""
    return "\n".join([
        "*Novo Quality*",
        "------------------",
        _novo_master_line(settings),
        f"Advisory Only: `{_novo_state(settings, 'novo_trade_quality_advisory_only', True)}`",
        f"No Hard Blocks: `{_novo_state(settings, 'novo_no_hard_blocks', True)}`",
        f"Trade Quality Module: `{_novo_state(settings, 'novo_trade_quality_enabled', True)}`",
        _novo_collecting_line(settings),
        "Quality Score: `COLLECTING`",
        "No runtime candidate feed is wired yet.",
    ])


def render_novo_liquidity_panel(settings=None) -> str:
    """Read-only Novo liquidity diagnostics panel."""
    return _render_novo_module_panel(
        settings,
        title="Novo Liquidity",
        flag_key="novo_liquidity_sweep_enabled",
        module_key="liquidity_sweep",
        label="Liquidity Sweep",
        empty_label="COLLECTING / NO SWEEP DATA YET",
    )


def render_novo_crt_panel(settings=None) -> str:
    """Read-only Novo CRT diagnostics panel."""
    return _render_novo_module_panel(
        settings,
        title="Novo CRT",
        flag_key="novo_crt_enabled",
        module_key="crt",
        label="CRT",
        empty_label="COLLECTING / NO CRT DATA YET",
    )


def render_novo_bias_panel(settings=None) -> str:
    """Read-only Novo 4H bias diagnostics panel."""
    return _render_novo_module_panel(
        settings,
        title="Novo 4H Bias",
        flag_key="novo_4h_bias_enabled",
        module_key="bias_4h",
        label="4H Bias",
        empty_label="COLLECTING / NO 4H BIAS DATA YET",
    )


def render_novo_learning_panel(settings=None) -> str:
    """Read-only Novo learning/memory diagnostics panel."""
    memory_enabled = bool(_setting_value(settings, "novo_ai_trade_memory_enabled", False))
    enrichment_telemetry = bool(_setting_value(settings, "novo_enrichment_outcome_telemetry_enabled", False))
    lines = [
        "*Novo Learning*",
        "------------------",
        _novo_master_line(settings),
        f"AI Trade Memory: `{'ON' if memory_enabled else 'DISABLED'}`",
        f"Enrichment Outcome Telemetry: `{'ON' if enrichment_telemetry else 'DISABLED'}`",
    ]
    try:
        import novo_trade_memory as _novo_memory
        snap = _novo_memory.summarize_memory(limit=500)
        summary = dict(snap.get("summary", {}) or {}) if isinstance(snap, dict) else {}
        total = int(summary.get("total_records", 0) or 0)
        if not memory_enabled or total <= 0:
            lines.extend([
                "Status: `COLLECTING / NO MEMORY YET`",
                "Records: `0`",
            ])
        else:
            lines.extend([
                "Status: `OK`",
                f"Records: `{total}`",
                f"Outcomes: `{int(summary.get('records_with_outcome', 0) or 0)}`",
                f"Wins/Losses: `{int(summary.get('win_count', 0) or 0)} / {int(summary.get('loss_count', 0) or 0)}`",
                f"Total PnL: `{float(summary.get('total_pnl', 0.0) or 0.0):.4f}`",
                f"Avg Quality: `{summary.get('average_quality_score')}`",
            ])
        if not enrichment_telemetry:
            lines.append("Enrichment Outcomes: `DISABLED`")
        elif total <= 0:
            lines.append("Enrichment Outcomes: `COLLECTING / NO OUTCOME DATA YET`")
        else:
            tier_counts = summary.get("quality_tier_counts", {}) or {}
            best_symbols = summary.get("best_symbols_by_pnl", []) or []
            worst_symbols = summary.get("worst_symbols_by_pnl", []) or []
            best_engines = summary.get("best_engines_by_pnl", []) or []
            worst_engines = summary.get("worst_engines_by_pnl", []) or []

            def _fmt_rank(rows) -> str:
                parts = []
                for row in list(rows)[:3]:
                    try:
                        if isinstance(row, dict):
                            name, pnl = row.get("name"), row.get("pnl")
                        else:
                            name, pnl = row
                        parts.append(f"{name}:{float(pnl):+.4f}")
                    except Exception:
                        continue
                return ", ".join(parts) if parts else "none"

            tier_text = ", ".join(f"{k}:{v}" for k, v in sorted(dict(tier_counts).items())) if tier_counts else "none"
            lines.extend([
                "Enrichment Outcomes: `OK`",
                f"Outcome Records: `{total}`",
                f"Outcomes: `{int(summary.get('records_with_outcome', 0) or 0)}`",
                f"Wins/Losses: `{int(summary.get('win_count', 0) or 0)} / {int(summary.get('loss_count', 0) or 0)}`",
                f"Total PnL: `{float(summary.get('total_pnl', 0.0) or 0.0):+.4f}`",
                f"Avg Quality: `{summary.get('average_quality_score')}`",
                f"Tier Counts: `{tier_text}`",
                f"Best Symbols: `{_fmt_rank(best_symbols)}`",
                f"Worst Symbols: `{_fmt_rank(worst_symbols)}`",
                f"Best Engines: `{_fmt_rank(best_engines)}`",
                f"Worst Engines: `{_fmt_rank(worst_engines)}`",
            ])
        errors = list(snap.get("errors", []) or []) if isinstance(snap, dict) else []
        if errors:
            lines.append(f"Read Warnings: `{len(errors)}`")
    except Exception as exc:
        lines.extend([
            "Status: `COLLECTING / NO MEMORY YET`",
            "Enrichment Outcomes: `COLLECTING / NO OUTCOME DATA YET`" if enrichment_telemetry else "Enrichment Outcomes: `DISABLED`",
            f"Read Error: `{type(exc).__name__}`",
        ])
    lines.append("Read-only panel. No memory is written by this command.")
    return "\n".join(lines)


def render_novo_gems_panel(settings=None) -> str:
    """Read-only Gem Hunter diagnostics panel."""
    gem_enabled = bool(_setting_value(settings, "novo_gem_hunter_enabled", False))
    alert_only = bool(_setting_value(settings, "gem_hunter_alert_only", True))
    lines = [
        "*Novo Gem Hunter*",
        "------------------",
        f"Gem Hunter: `{'ENABLED' if gem_enabled else 'DISABLED'}`",
        f"Alert Only: `{'ON' if alert_only else 'OFF'}`",
        "Status: `COLLECTING / NO GEM SCAN DATA YET`",
        "Runtime Scan: `NOT WIRED`",
    ]
    if not alert_only:
        lines.append("Warning: alert-only setting is OFF, but Telegram panel remains display-only.")
    lines.append("Read-only panel. No scans, trade intents, or orders are created by this command.")
    return "\n".join(lines)


HELP_MAIN_TEXT = "\n".join([
    "*OxyPulse Help*",
    "------------------",
    "Use these guides for commands, risk behavior, the V6 lifecycle, and engine roles.",
    "All live controls are under Settings. Diagnostics is read-only inspection.",
])

HELP_COMMANDS_TEXT = "\n".join([
    "*Commands*",
    "------------------",
    "/start - open the dashboard",
    "/help - open this help panel",
    "/status - show runtime status",
    "/trades - show recent trade activity",
    "/positions - show open positions",
    "/settings - open live controls",
])

HELP_RISK_TEXT = "\n".join([
    "*Risk*",
    "------------------",
    "Risk controls size new entries, cap exposure, protect open positions, and monitor drawdown.",
    "Settings contains live controls. Diagnostics shows current state and blocks without changing behavior.",
    "Break-even, trailing, partials, profit lock, early invalidation, and repeat-loss cooling are execution lifecycle controls.",
])

HELP_V6_TEXT = "\n".join([
    "*V6 Guide*",
    "------------------",
    "OxyPulse scans enabled engines, scores candidates, applies admission controls, places OKX orders, registers positions, and manages exits through the registry.",
    "Runtime reports should reflect persisted tracker state, live registry state, and realized close records rather than stale panel memory.",
    "Telegram Settings is the control plane. Telegram Diagnostics is the inspection plane.",
])


def _engine_help(name: str, role: str, lane: str, strengths: str, weaknesses: str, ideal: str, entry: str, avoids: str) -> str:
    return "\n".join([
        f"*{name} Engine Guide*",
        "------------------",
        f"Role: {role}",
        f"Lane: {lane}",
        f"Strengths: {strengths}",
        f"Weaknesses: {weaknesses}",
        f"Ideal Market: {ideal}",
        f"Entry Summary: {entry}",
        f"Avoids Trading: {avoids}",
    ])


HELP_SHOGUN_TEXT = _engine_help(
    "SHOGUN",
    "Structure and displacement specialist.",
    "SMC / ICT-style continuation and reversal structure.",
    "Good at clean breaks, fair-value displacement, and structure confirmation.",
    "Can over-wait in noisy chop or when structure is ambiguous.",
    "Liquid symbols with clear swing structure and decisive displacement.",
    "Looks for structure alignment, entry-zone reaction, and confirmation before admission.",
    "Thin liquidity, poor confirmation, or unclear structure.",
)
HELP_SUSANOO_TEXT = _engine_help(
    "SUSANOO",
    "Funding and squeeze opportunity scanner.",
    "Derivatives pressure / squeeze lane.",
    "Finds crowding and funding-driven unwind setups.",
    "Weak when funding pressure is present but price confirmation is missing.",
    "Crowded perpetual markets with confirmed directional release.",
    "Combines funding pressure, momentum, and market-quality checks.",
    "Flat pressure, weak liquidity, or no confirmation path.",
)
HELP_FUJIN_TEXT = _engine_help(
    "FUJIN",
    "Order-flow and fast continuation engine.",
    "Flow and momentum lane.",
    "Responsive to volume, liquidity, and short-lived continuation windows.",
    "Can be sensitive to spread changes and fake bursts.",
    "Liquid symbols with strong flow and clean continuation.",
    "Uses flow support, price movement, and execution quality to qualify entries.",
    "Spread deterioration, weak volume, or conflicting flow.",
)
HELP_AMATERASU_TEXT = _engine_help(
    "AMATERASU",
    "Multi-timeframe confluence engine.",
    "Trend confluence lane.",
    "Strong when higher and lower timeframes agree.",
    "Slower to react when markets rotate quickly.",
    "Stable trends with aligned momentum and structure.",
    "Requires stacked confluence across timeframe, momentum, and trend checks.",
    "Mixed timeframe signals or unstable regime.",
)
HELP_OROCHI_TEXT = _engine_help(
    "OROCHI",
    "Liquid trend pullback engine.",
    "Trend pullback lane.",
    "Good at entering pullbacks inside established liquid trends.",
    "Weak when pullbacks become reversals.",
    "Liquid trending markets with orderly retracements.",
    "Waits for pullback quality, continuation support, and acceptable execution conditions.",
    "Broken structure, poor liquidity, or choppy regime.",
)
HELP_TSUNAMI_TEXT = _engine_help(
    "TSUNAMI",
    "Liquidation cascade engine.",
    "Volatility expansion lane.",
    "Captures liquidation-driven directional bursts.",
    "Can be noisy after the first cascade impulse fades.",
    "High-volume liquidation events with follow-through.",
    "Looks for liquidation pressure, momentum expansion, and continuation quality.",
    "No follow-through, spread stress, or exhausted move.",
)
HELP_SUIJIN_TEXT = _engine_help(
    "SUIJIN",
    "VWAP and mean-reversion specialist.",
    "Reversion lane.",
    "Works well when price overextends from fair value and liquidity remains healthy.",
    "Weak in runaway trends.",
    "Liquid ranging or stretched markets that respect VWAP/fair value.",
    "Combines deviation, reversion evidence, and market-quality checks.",
    "Strong breakout continuation or failed reversion evidence.",
)
HELP_RAIJUU_TEXT = _engine_help(
    "RAIJUU",
    "Open-interest divergence scanner.",
    "OI divergence lane.",
    "Finds price/OI disagreement that can precede directional resolution.",
    "Can be early when divergence persists.",
    "Liquid derivatives markets with clear OI divergence and price confirmation.",
    "Requires OI divergence, price response, and confirmation before admission.",
    "No OI signal, weak confirmation, or unstable liquidity.",
)


def kb_help() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Commands", callback_data="help_commands"), InlineKeyboardButton("Risk", callback_data="help_risk")],
        [InlineKeyboardButton("V6 Guide", callback_data="help:v6guide")],
        [InlineKeyboardButton("SHOGUN", callback_data="help_shogun"), InlineKeyboardButton("SUSANOO", callback_data="help_susanoo")],
        [InlineKeyboardButton("FUJIN", callback_data="help_fujin"), InlineKeyboardButton("AMATERASU", callback_data="help_amaterasu")],
        [InlineKeyboardButton("OROCHI", callback_data="help_orochi"), InlineKeyboardButton("TSUNAMI", callback_data="help_tsunami")],
        [InlineKeyboardButton("SUIJIN", callback_data="help_suijin"), InlineKeyboardButton("RAIJUU", callback_data="help_raijuu")],
        [InlineKeyboardButton("Home", callback_data="main_menu")],
    ])


def kb_settings_hub(settings) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Trading", callback_data="set:cat:trading"), InlineKeyboardButton("Strategy", callback_data="set:cat:strategy")],
        [InlineKeyboardButton("Risk", callback_data="set:cat:risk"), InlineKeyboardButton("Execution", callback_data="set:cat:execution")],
        [InlineKeyboardButton("Capital", callback_data="set:cat:capital"), InlineKeyboardButton("Diagnostics Controls", callback_data="set:cat:diagnostics")],
        [InlineKeyboardButton("System", callback_data="set:cat:system")],
        [InlineKeyboardButton("Back", callback_data="main_menu")],
    ])


def panel_settings_cat_execution_text(settings) -> str:
    return "\n".join([
        "*Settings - Execution*",
        DIV,
        "Exit timing, lifecycle protection, partials, runner behavior, and localized repeat-loss cooling.",
        f"Adaptive Early Invalidation: `{_setting_state(settings, 'adaptive_early_invalidation_enabled')}`",
        f"Near-TP Grace: `{_setting_state(settings, 'early_invalidation_near_tp_grace_enabled', True)}`",
        f"Repeat Loss Cooling: `{_setting_state(settings, 'symbol_direction_loss_cooldown_enabled')}`",
    ])


def kb_settings_cat_execution(settings) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Early Invalidation", callback_data="set:fg:early_invalidation")],
        [InlineKeyboardButton("Trailing", callback_data="set:fg:trailing"), InlineKeyboardButton("Break-even", callback_data="set:fg:breakeven")],
        [InlineKeyboardButton("Partials", callback_data="set:fg:partials"), InlineKeyboardButton("Profit Lock", callback_data="set:fg:profit_lock")],
        [InlineKeyboardButton("Repeat Loss Cooling", callback_data="set:fg:repeat_loss_cooling")],
        [InlineKeyboardButton("TP Mode", callback_data="set:room:tp_mode")],
        [InlineKeyboardButton("Settings", callback_data="settings_menu")],
    ])


def panel_fg_early_invalidation_text(settings) -> str:
    return "\n".join([
        "*Early Invalidation*",
        DIV,
        "Adaptive lifecycle exit controls. Bad trades can still die quickly; earned or high-quality trades get patience.",
        f"Master Enabled: `{_setting_state(settings, 'early_invalidation_enabled', True)}`",
        f"Adaptive Enabled: `{_setting_state(settings, 'adaptive_early_invalidation_enabled')}`",
        f"Invalidation Telemetry: `{_setting_state(settings, 'profitability_telemetry_enabled', True)}`",
        f"Near-TP Grace: `{_setting_state(settings, 'early_invalidation_near_tp_grace_enabled', True)}`",
        f"Base/Floor/Cap Hold: `{_setting_num(settings, 'early_invalidation_base_min_hold_sec', 45, 0)}s / {_setting_num(settings, 'early_invalidation_min_hold_floor_sec', 20, 0)}s / {_setting_num(settings, 'early_invalidation_max_hold_cap_sec', 720, 0)}s`",
        f"Failure/Patience Thresholds: `{_setting_num(settings, 'early_invalidation_failure_score_threshold', 2.5)} / {_setting_num(settings, 'early_invalidation_patience_score_threshold', 1.5)}`",
        f"Near-TP Ratio/Grace: `{_setting_num(settings, 'early_invalidation_near_tp_floor_ratio', 0.85)} / {_setting_num(settings, 'early_invalidation_near_tp_grace_sec', 90, 0)}s`",
        f"Chop/Trend Multipliers: `{_setting_num(settings, 'early_invalidation_chop_strictness_mult', 1.25)} / {_setting_num(settings, 'early_invalidation_trend_patience_mult', 1.35)}`",
    ])


def kb_fg_early_invalidation(settings) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Master Enable", callback_data="set:room:early_invalidation_enabled")],
        [InlineKeyboardButton("Adaptive Enable", callback_data="set:room:adaptive_early_invalidation_enabled")],
        [InlineKeyboardButton("Invalidation Telemetry", callback_data="set:room:profitability_telemetry_enabled")],
        [InlineKeyboardButton("Near-TP Grace", callback_data="set:room:early_invalidation_near_tp_grace_enabled")],
        [InlineKeyboardButton("Base Hold", callback_data="set:room:early_invalidation_base_min_hold_sec"), InlineKeyboardButton("Hold Floor", callback_data="set:room:early_invalidation_min_hold_floor_sec")],
        [InlineKeyboardButton("Hold Cap", callback_data="set:room:early_invalidation_max_hold_cap_sec")],
        [InlineKeyboardButton("Failure Score", callback_data="set:room:early_invalidation_failure_score_threshold"), InlineKeyboardButton("Patience Score", callback_data="set:room:early_invalidation_patience_score_threshold")],
        [InlineKeyboardButton("Near-TP Ratio", callback_data="set:room:early_invalidation_near_tp_floor_ratio"), InlineKeyboardButton("Grace Seconds", callback_data="set:room:early_invalidation_near_tp_grace_sec")],
        [InlineKeyboardButton("Chop Strictness", callback_data="set:room:early_invalidation_chop_strictness_mult"), InlineKeyboardButton("Trend Patience", callback_data="set:room:early_invalidation_trend_patience_mult")],
        [InlineKeyboardButton("View Runtime Values", callback_data="diag:control_state")],
        [InlineKeyboardButton("Execution", callback_data="set:cat:execution")],
    ])


def panel_fg_repeat_loss_cooling_text(settings) -> str:
    return "\n".join([
        "*Repeat Loss Cooling*",
        DIV,
        "Localized cooling for repeated same-symbol plus same-direction losses only. It is not a global trade suppressor.",
        f"Enabled: `{_setting_state(settings, 'symbol_direction_loss_cooldown_enabled')}`",
        f"Loss Threshold: `{_setting_num(settings, 'symbol_direction_loss_threshold', 2, 0)}`",
        f"Cooldown Minutes: `{_setting_num(settings, 'symbol_direction_cooldown_minutes', 30, 0)}`",
        f"Confidence Bump: `{_setting_num(settings, 'symbol_direction_loss_conf_bump', 0.6)}`",
        f"Size Multiplier: `{_setting_num(settings, 'symbol_direction_loss_size_mult', 0.75)}`",
        f"Memory Decay Hours: `{_setting_num(settings, 'symbol_direction_memory_decay_hours', 24, 0)}`",
    ])


def kb_fg_repeat_loss_cooling(settings) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Enable", callback_data="set:room:symbol_direction_loss_cooldown_enabled")],
        [InlineKeyboardButton("Loss Threshold", callback_data="set:room:symbol_direction_loss_threshold"), InlineKeyboardButton("Cooldown Duration", callback_data="set:room:symbol_direction_cooldown_minutes")],
        [InlineKeyboardButton("Confidence Bump", callback_data="set:room:symbol_direction_loss_conf_bump"), InlineKeyboardButton("Size Multiplier", callback_data="set:room:symbol_direction_loss_size_mult")],
        [InlineKeyboardButton("Memory Decay", callback_data="set:room:symbol_direction_memory_decay_hours")],
        [InlineKeyboardButton("Execution", callback_data="set:cat:execution")],
    ])


def panel_fg_trailing_text(settings) -> str:
    return "\n".join([
        "*Trailing*",
        DIV,
        "Trailing activation, runner preservation, continuation runner controls, and exhaustion tightening.",
        f"Trailing Enabled: `{_setting_state(settings, 'tsl_enabled', True)}`",
        f"Activation/Distance: `{_setting_num(settings, 'trailing_activation_pct', 2.0)}% / {_setting_num(settings, 'trailing_distance_pct', 1.0)}%`",
        f"TP1/TP2 Trail: `{_setting_num(settings, 'trailing_after_tp1_pct', 0.85)}% / {_setting_num(settings, 'trailing_after_tp2_pct', 0.65)}%`",
        f"Runner Core: `{_setting_num(settings, 'runner_core_min_remaining_pct', 45, 1)}%`",
        f"Continuation Runner: `{_setting_state(settings, 'continuation_runner_enabled', True)}` core `{_setting_num(settings, 'continuation_runner_core_min_pct', 55, 1)}%`",
        f"Exhaustion Tightening: `{_setting_state(settings, 'exhaustion_assisted_tightening_enabled')}`",
    ])


def kb_fg_trailing(settings) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Enable Trailing", callback_data="set:room:tsl_enabled")],
        [InlineKeyboardButton("Activation", callback_data="set:room:trailing_activation_pct"), InlineKeyboardButton("Distance", callback_data="set:room:trailing_distance_pct")],
        [InlineKeyboardButton("After TP1", callback_data="set:room:trailing_after_tp1_pct"), InlineKeyboardButton("After TP2", callback_data="set:room:trailing_after_tp2_pct")],
        [InlineKeyboardButton("Runner Core", callback_data="set:room:runner_core_min_remaining_pct")],
        [InlineKeyboardButton("Continuation Enable", callback_data="set:room:continuation_runner_enabled"), InlineKeyboardButton("Continuation Core", callback_data="set:room:continuation_runner_core_min_pct")],
        [InlineKeyboardButton("Continuation ROI", callback_data="set:room:continuation_runner_min_roi"), InlineKeyboardButton("Continuation Score", callback_data="set:room:continuation_runner_score_threshold")],
        [InlineKeyboardButton("TP2 Close Mult", callback_data="set:room:continuation_runner_tp2_close_mult"), InlineKeyboardButton("Trail Mult", callback_data="set:room:continuation_runner_trail_mult")],
        [InlineKeyboardButton("Exhaustion Enable", callback_data="set:room:exhaustion_assisted_tightening_enabled")],
        [InlineKeyboardButton("Exhaustion Score", callback_data="set:room:exhaustion_tighten_score_threshold"), InlineKeyboardButton("Exhaustion Trail", callback_data="set:room:exhaustion_tighten_trail_mult")],
        [InlineKeyboardButton("Execution", callback_data="set:cat:execution")],
    ])


def panel_fg_partials_text(settings) -> str:
    return "\n".join([
        "*Partials*",
        DIV,
        "TP trims, fee-aware deferral, economic minimum protection, and near-floor TP earned controls.",
        f"Partials Enabled: `{_setting_state(settings, 'partial_exit_enabled', True)}`",
        f"TP1/TP2 Trigger: `{_setting_num(settings, 'tp1_trigger_pct', 2.0)}% / {_setting_num(settings, 'tp2_trigger_pct', 4.0)}%`",
        f"TP1/TP2 Close: `{_setting_num(settings, 'partial_tp1_close_pct', 35, 1)}% / {_setting_num(settings, 'partial_tp2_close_pct', 35, 1)}%`",
        f"Fee-Aware Deferral: `{_setting_state(settings, 'fee_aware_partial_deferral_enabled', True)}`",
        f"Min Partial/Full Profit: `${_setting_num(settings, 'min_net_take_profit_usdt', 0.03)} / ${_setting_num(settings, 'min_full_close_profit_usdt', 0.05)}`",
        f"Runner Notional/Fee Mult: `${_setting_num(settings, 'economic_min_runner_notional_usdt', 5.0)} / {_setting_num(settings, 'economic_min_fee_mult', 8.0)}`",
        f"Near-TP Earned Ratio: `{_setting_num(settings, 'early_invalidation_near_tp_floor_ratio', 0.85)}`",
    ])


def kb_fg_partials(settings) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Enable Partials", callback_data="set:room:partial_exit_enabled")],
        [InlineKeyboardButton("TP1 Trigger", callback_data="set:room:tp1_trigger_pct"), InlineKeyboardButton("TP1 Close", callback_data="set:room:partial_tp1_close_pct")],
        [InlineKeyboardButton("TP2 Trigger", callback_data="set:room:tp2_trigger_pct"), InlineKeyboardButton("TP2 Close", callback_data="set:room:partial_tp2_close_pct")],
        [InlineKeyboardButton("Fee-Aware Deferral", callback_data="set:room:fee_aware_partial_deferral_enabled")],
        [InlineKeyboardButton("Min Partial Profit", callback_data="set:room:min_net_take_profit_usdt"), InlineKeyboardButton("Min Full Profit", callback_data="set:room:min_full_close_profit_usdt")],
        [InlineKeyboardButton("Runner Notional", callback_data="set:room:economic_min_runner_notional_usdt"), InlineKeyboardButton("Fee Multiplier", callback_data="set:room:economic_min_fee_mult")],
        [InlineKeyboardButton("Deferred Log Cooldown", callback_data="set:room:partial_skip_log_cooldown_sec")],
        [InlineKeyboardButton("Near-TP Grace", callback_data="set:room:early_invalidation_near_tp_grace_enabled"), InlineKeyboardButton("Near-TP Ratio", callback_data="set:room:early_invalidation_near_tp_floor_ratio")],
        [InlineKeyboardButton("Execution", callback_data="set:cat:execution")],
    ])


def panel_positions_truth_text(snapshot: dict | None) -> str:
    snapshot = snapshot or {}
    positions = snapshot.get("positions", {}) or {}
    local_unknown_count = int(snapshot.get("local_unknown_count", 0) or 0)
    local_active_count = int(snapshot.get("local_active_count", 0) or 0)
    local_unknown_symbols = ", ".join(str(s) for s in snapshot.get("local_unknown_symbols", []) or [])
    if not positions:
        if local_unknown_count:
            return (
                "*Positions*\n\n"
                f"Active: `{local_active_count}`\n"
                f"⚠️ Unknown exposure: `{local_unknown_count}` ({local_unknown_symbols or 'unidentified'})\n"
                "_Readiness is blocked until local exposure truth is resolved._"
            )
        return "*Positions*\n\n_No open positions._"
    lines = [f"*Positions* · `{len(positions)}` open", DIV]
    lines.append(f"Local active: `{local_active_count}` · Unknown exposure: `{local_unknown_count}`")
    if local_unknown_count:
        lines.append(f"⚠️ Unresolved local exposure: `{local_unknown_symbols or 'unidentified'}`")
    total = snapshot.get("open_pnl_usdt")
    label = snapshot.get("source_label") or _truth_source_label(snapshot)
    if total is None:
        lines.append(f"Open PnL: `—` ({label}, {_truth_age(snapshot)})")
    else:
        lines.append(f"Open PnL: {_fmt_pnl(float(total))} ({label}, {_truth_age(snapshot)})")
    configured_mode = normalize_tp_mode(snapshot.get("configured_tp_mode", "reference_only"))
    lines.append(f"TP mode current: `{configured_mode.upper()}`")
    for symbol, row in sorted(positions.items()):
        side = "LONG" if str(row.get("side", "")).lower() == "long" else "SHORT"
        src = row.get("source_label") or _truth_source_label(row)
        pnl = row.get("unrealised_pnl")
        pnl_text = _fmt_money_or_dash(pnl, 4) if pnl is not None else "`—`"
        roi = row.get("roi_pct")
        roi_text = f"`{float(roi):+.2f}%`" if roi is not None else "`—`"
        reg = row.get("registry", {}) or {}
        persisted_owner = normalize_persisted_owner(reg.get("tp_owner", "")) or UNRESOLVED
        mode_at_entry = normalize_tp_mode(reg.get("tp_mode_at_entry", ""), default="UNRESOLVED")
        sl = reg.get("stop_loss")
        desired_sl = reg.get("desired_stop_price", sl)
        local_effective_sl = reg.get("local_effective_stop_price")
        exchange_sl = reg.get("exchange_confirmed_stop_price")
        protection_state = str(reg.get("exchange_protection_status", "") or "")
        tp = reg.get("take_profit")
        partial_states = reg.get("partial_exit_state", {}) or {}
        latest_partial = next(iter(reversed(list(partial_states.values()))), {}) if isinstance(partial_states, dict) else {}
        partial_text = ""
        if latest_partial:
            partial_text = (
                f"\n  Partial `{latest_partial.get('stage', 'partial')}` "
                f"state `{latest_partial.get('lifecycle', 'UNKNOWN')}` "
                f"filled `{float(latest_partial.get('verified_filled_contracts', 0.0) or 0.0):.8f}` "
                f"remaining `{float(latest_partial.get('remaining_exchange_contracts', 0.0) or 0.0):.8f}`"
            )
        exposure_text = (
            f"\n  Exposure `{reg.get('exposure_truth_status', 'ACTIVE_POSITION')}` "
            f"contracts `{float(reg.get('confirmed_remaining_contracts', 0.0) or 0.0):.8f}` "
            f"at `{reg.get('exchange_position_timestamp') or '—'}`"
        )
        mismatch = str(row.get("mismatch_state", "") or "")
        warn = "" if mismatch in {"", "OK"} else f"\n  Warn `{mismatch[:90]}`"
        requested_lev = row.get("requested_leverage")
        configured_lev = row.get("exchange_configured_leverage")
        effective_lev = row.get("position_effective_leverage")
        truth_status = str(row.get("leverage_truth_status", "") or "").upper()
        if effective_lev is None and truth_status not in {"LEVERAGE_UNKNOWN", "STALE", "UNKNOWN"}:
            effective_lev = row.get("leverage")
        effective_text = (
            f"{int(float(effective_lev))}x"
            if effective_lev is not None and float(effective_lev or 0) > 0
            else "UNKNOWN"
        )
        leverage_text = (
            f"  Leverage requested `{int(float(requested_lev)) if requested_lev is not None else '—'}x` "
            f"configured `{int(float(configured_lev)) if configured_lev is not None else '—'}x` "
            f"exchange/effective `{effective_text}`\n"
        )
        lines.append(
            f"\n`{symbol}` {side} {src}\n"
            f"  Size `{row.get('size') or '—'}` Notional {_fmt_price_or_dash(row.get('notional_usd'))} Margin {_fmt_price_or_dash(row.get('margin'))}\n"
            f"{leverage_text}  Entry {_fmt_price_or_dash(row.get('entry_price'))} Mark {_fmt_price_or_dash(row.get('mark_price'))}\n"
            f"  PnL {pnl_text} ROI {roi_text} Age `{float(row.get('age_sec', 0.0) or 0.0):.0f}s` Conf `{row.get('confidence','LOW')}`\n"
            f"  TP mode-at-entry `{mode_at_entry.upper()}` owner `{persisted_owner}`\n"
            f"  SL desired {_fmt_price_or_dash(desired_sl)} local {_fmt_price_or_dash(local_effective_sl)} exchange {_fmt_price_or_dash(exchange_sl)}"
            f" state `{protection_state or 'UNKNOWN'}` TP {_fmt_price_or_dash(tp)}{exposure_text}{partial_text}{warn}"
        )
    return "\n".join(lines)


def panel_pnl_sync_text(snapshot: dict | None, symbol: str | None = None) -> str:
    snapshot = snapshot or {}
    positions = snapshot.get("positions", {}) or {}
    if symbol:
        row = positions.get(str(symbol).upper())
        if not row:
            return f"*Position Debug*\n{DIV}\n`{str(symbol).upper()}` not found in current snapshot."
        registry = row.get("registry", {}) or {}
        owner = normalize_persisted_owner(registry.get("tp_owner", "")) or UNRESOLVED
        mode_at_entry = normalize_tp_mode(registry.get("tp_mode_at_entry", ""), default="UNRESOLVED")
        return "\n".join([
            f"*Position Debug · {str(symbol).upper()}*",
            DIV,
            f"Source: `{row.get('source_label') or _truth_source_label(row)}`",
            f"State: `{row.get('status','UNKNOWN')}` Conf `{row.get('confidence','LOW')}` Age `{float(row.get('age_sec', 0.0) or 0.0):.0f}s`",
            f"OKX upl: {_fmt_money_or_dash(row.get('okx_upl'), 4)}",
            f"Local est: {_fmt_money_or_dash(row.get('local_estimate'), 4)}",
            f"Drift: {_fmt_money_or_dash(row.get('drift_usdt'), 4)} `{row.get('drift_pct','—')}%`",
            f"Entry: {_fmt_price_or_dash(row.get('entry_price'))}",
            f"Mark: {_fmt_price_or_dash(row.get('mark_price'))}",
            f"TP mode current: `{normalize_tp_mode((snapshot or {}).get('configured_tp_mode', 'reference_only')).upper()}`",
            f"TP mode at entry: `{mode_at_entry.upper()}`",
            f"TP owner: `{owner}`",
            f"Mismatch: `{str(row.get('mismatch_state','OK'))[:120]}`",
        ])
    lines = [
        "*PnL Sync*",
        DIV,
        f"Source: `{snapshot.get('source_label') or _truth_source_label(snapshot)}` Conf `{snapshot.get('confidence','LOW')}`",
        f"OKX Open PnL: {_fmt_money_or_dash(snapshot.get('open_pnl_usdt'), 4)}",
        f"Local Estimate: {_fmt_money_or_dash(snapshot.get('local_estimate_pnl_usdt'), 4)}",
        f"Drift: {_fmt_money_or_dash(snapshot.get('drift_usdt'), 4)}",
        f"Age: `{float(snapshot.get('age_sec', 0.0) or 0.0):.0f}s`",
        f"Stale: `{int(snapshot.get('stale_count', 0) or 0)}` Mismatch: `{int(snapshot.get('mismatch_count', 0) or 0)}`",
        f"TP mode current: `{normalize_tp_mode(snapshot.get('configured_tp_mode', 'reference_only')).upper()}`",
    ]
    if positions:
        lines.append("")
        lines.append("*Positions:*")
        for sym, row in sorted(positions.items()):
            registry = row.get("registry", {}) or {}
            exposure = str(registry.get("exposure_truth_status", "") or "")
            contracts = registry.get("confirmed_remaining_contracts")
            partial_states = registry.get("partial_exit_state", {}) or {}
            latest_partial = next(iter(reversed(list(partial_states.values()))), {}) if isinstance(partial_states, dict) else {}
            exposure_text = (
                f" exposure `{exposure}` contracts `{float(contracts or 0.0):.8f}`"
                if exposure else ""
            )
            partial_text = (
                f" partial `{latest_partial.get('lifecycle', 'UNKNOWN')}`"
                if latest_partial else ""
            )
            owner = normalize_persisted_owner(registry.get("tp_owner", "")) or UNRESOLVED
            mode_at_entry = normalize_tp_mode(registry.get("tp_mode_at_entry", ""), default="UNRESOLVED")
            lines.append(
                f"`{sym}` {row.get('source_label') or _truth_source_label(row)} "
                f"state `{row.get('status','UNKNOWN')}` pnl {_fmt_money_or_dash(row.get('unrealised_pnl'), 4)} "
                f"drift {_fmt_money_or_dash(row.get('drift_usdt'), 4)} conf `{row.get('confidence','LOW')}`"
                f" tp_mode_at_entry `{mode_at_entry.upper()}` owner `{owner}`{exposure_text}{partial_text}"
            )
    else:
        lines.append("")
        lines.append("_No open positions._")
    return "\n".join(lines)


def kb_pnl_sync(snapshot: dict | None) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    positions = (snapshot or {}).get("positions", {}) or {}
    for sym in sorted(positions)[:8]:
        rows.append([InlineKeyboardButton(f"Debug {sym}", callback_data=f"diag:pnl_position:{sym}")])
    rows.append([InlineKeyboardButton("Refresh", callback_data="diag:pnl_sync"),
                 InlineKeyboardButton("Back to Diagnostics", callback_data="s:diagnostics")])
    return InlineKeyboardMarkup(rows)


def panel_shadow_rehab_text(settings) -> str:
    try:
        import shadow_rehab_tracker as _rehab
        snap = _rehab.get_rehab_snapshot(settings)
    except Exception:
        snap = {"engines": []}
    lines = [
        "Shadow Engine Rehab",
        DIV,
        "Read-only post-upgrade shadow/probation telemetry.",
        "No promotion, sizing, risk, or execution controls here.",
        "",
    ]
    engines = list(snap.get("engines", []) or [])
    if not engines:
        lines.append("Status: `INSUFFICIENT_DATA`")
    for row in engines[:8]:
        metric = row.get("post_upgrade", {}) or {}
        engine = str(row.get("engine", "?") or "?")
        mode = str(row.get("mode", "UNKNOWN") or "UNKNOWN")
        status = str(row.get("status", "INSUFFICIENT_DATA") or "INSUFFICIENT_DATA")
        lines.append(f"`{engine}` mode `{mode}` status `{status}` prob `{row.get('probation_status', 'NONE')}`")
        lines.append(
            f"  trades `{int(metric.get('trades', 0) or 0)}` "
            f"WR `{float(metric.get('wr', 0.0) or 0.0):.1f}%` "
            f"PF `{float(metric.get('pf', 0.0) or 0.0):.2f}` "
            f"Exp `{float(metric.get('expectancy', 0.0) or 0.0):+.4f}` "
            f"Net `{float(metric.get('net_pnl', 0.0) or 0.0):+.4f}`"
        )
        lines.append(
            f"  MFE `{float(metric.get('avg_mfe', 0.0) or 0.0):+.2f}%` "
            f"MAE `{float(metric.get('avg_mae', 0.0) or 0.0):+.2f}%` "
            f"signals `{int(row.get('total_signals', 0) or 0)}` "
            f"pending `{int(row.get('pending_signals', 0) or 0)}`"
        )
    return "\n".join(lines)[:3900]


def panel_izanagi_outcomes_text(settings=None) -> str:
    try:
        import shadow_rehab_tracker as _rehab
        snap = _rehab.get_izanagi_snapshot()
    except Exception:
        snap = {"signals": 0, "pending_1h": 0, "horizons": {}, "best_symbols": {}, "worst_symbols": {}, "status": "INSUFFICIENT_DATA"}
    lines = [
        "IZANAGI Outcome Tracker",
        DIV,
        "Read-only virtual outcomes for advisor contributions.",
        "Telemetry only; no execution controls here.",
        "",
        f"Status: `{snap.get('status', 'INSUFFICIENT_DATA')}`",
        f"Advisor signals: `{int(snap.get('signals', 0) or 0)}` pending 1h `{int(snap.get('pending_1h', 0) or 0)}`",
        "",
    ]
    horizons = snap.get("horizons", {}) or {}
    for name in ("15m", "1h", "4h", "24h"):
        metric = horizons.get(name, {}) or {}
        lines.append(
            f"`{name}` trades `{int(metric.get('trades', 0) or 0)}` "
            f"WR `{float(metric.get('wr', 0.0) or 0.0):.1f}%` "
            f"PF `{float(metric.get('pf', 0.0) or 0.0):.2f}` "
            f"Exp `{float(metric.get('expectancy', 0.0) or 0.0):+.4f}` "
            f"MFE `{float(metric.get('avg_mfe', 0.0) or 0.0):+.2f}%` "
            f"MAE `{float(metric.get('avg_mae', 0.0) or 0.0):+.2f}%`"
        )
    best = snap.get("best_symbols", {}) or {}
    worst = snap.get("worst_symbols", {}) or {}
    if best:
        lines.append("")
        lines.append("Best symbols: " + ", ".join(f"`{k}` {v:+.4f}" for k, v in list(best.items())[:5]))
    if worst:
        lines.append("Worst symbols: " + ", ".join(f"`{k}` {v:+.4f}" for k, v in list(worst.items())[:5]))
    return "\n".join(lines)[:3900]


def _signaldiag_int(value, default: int = 0) -> int:
    try:
        return int(value if value is not None else default)
    except Exception:
        return int(default)


def _signaldiag_epoch(value) -> str:
    try:
        ts = float(value or 0.0)
        if ts <= 0:
            return "UNKNOWN"
        return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts))
    except Exception:
        return "UNKNOWN"


def _signaldiag_vote_rescue_success_count(limit: int = 200) -> int:
    try:
        import decision_trace as _trace
        rows = _trace.get_recent(limit=limit)
        count = 0
        for row in rows if isinstance(rows, list) else []:
            text = str(row)
            decision = str(row.get("decision", row.get("final_decision", "")) if isinstance(row, dict) else "")
            reason = str(row.get("reason", "") if isinstance(row, dict) else "")
            if (
                "VOTE_RESCUE_OPEN_FLOOR" in text
                or "vote_rescue" == decision
                or "VOTE_RESCUE_OPEN_FLOOR" == reason
            ):
                count += 1
        return count
    except Exception:
        return 0


def _signaldiag_float(value):
    try:
        return float(value)
    except Exception:
        return None


def _signaldiag_x_vs_y(value, threshold, suffix: str = "") -> str:
    left = _signaldiag_float(value)
    right = _signaldiag_float(threshold)
    if left is None and right is None:
        return "n/a"
    if left is None:
        return f"n/a/{right:.4g}{suffix}"
    if right is None:
        return f"{left:.4g}{suffix}/n/a"
    return f"{left:.4g}{suffix}/{right:.4g}{suffix}"


def _signaldiag_spread_liquidity_lines(limit: int = 50) -> list[str]:
    try:
        import category_engine as _category_engine
        getter = getattr(_category_engine, "get_spread_liquidity_block_diagnostics", None)
        if not callable(getter):
            return ["", "*Spread/Liquidity Blocks*", "- `COLLECTING`: detail capture unavailable"]
        snap = getter(limit=limit) or {}
        records = snap.get("records", []) if isinstance(snap, dict) else []
        if not isinstance(records, list):
            records = []
        total_recent = _signaldiag_int(snap.get("total_recent") if isinstance(snap, dict) else 0)
    except Exception:
        return ["", "*Spread/Liquidity Blocks*", "- `UNAVAILABLE`: read failed"]

    lines = ["", "*Spread/Liquidity Blocks*", f"- Recent Captured: `{total_recent}`"]
    if not records:
        lines.append("- `COLLECTING`: no detailed blocks captured yet")
        return lines

    def bump(counter: dict, key):
        item = str(key or "UNKNOWN").upper()
        counter[item] = _signaldiag_int(counter.get(item)) + 1

    symbols, engines, sub_reasons = {}, {}, {}
    for row in records:
        if not isinstance(row, dict):
            continue
        bump(symbols, row.get("symbol"))
        bump(engines, row.get("engine"))
        bump(sub_reasons, row.get("sub_reason"))

    top_symbols = ", ".join(f"`{k}` {v}" for k, v in sorted(symbols.items(), key=lambda x: x[1], reverse=True)[:5])
    top_engines = ", ".join(f"`{k}` {v}" for k, v in sorted(engines.items(), key=lambda x: x[1], reverse=True)[:5])
    top_sub = ", ".join(f"`{k.lower()}` {v}" for k, v in sorted(sub_reasons.items(), key=lambda x: x[1], reverse=True)[:5])
    if top_symbols:
        lines.append(f"- Top Symbols: {top_symbols}")
    if top_engines:
        lines.append(f"- Top Engines: {top_engines}")
    if top_sub:
        lines.append(f"- Top Reasons: {top_sub}")

    lines.append("- Latest Examples:")
    for row in records[-5:]:
        if not isinstance(row, dict):
            continue
        symbol = str(row.get("symbol") or "?").upper()
        engine = str(row.get("engine") or "?").upper()
        sub = str(row.get("sub_reason") or "unknown").lower()
        spread = _signaldiag_x_vs_y(row.get("spread_pct"), row.get("spread_threshold"), "%")
        volume = _signaldiag_x_vs_y(row.get("volume_24h"), row.get("volume_threshold"))
        lines.append(f"  - `{symbol}` `{engine}` `{sub}` spr `{spread}` vol `{volume}`")
    return lines


def render_silent_signal_diagnostics_panel(settings=None) -> str:
    """Read-only in-process silent signal diagnostics panel."""
    try:
        import category_engine as _category_engine
        getter = getattr(_category_engine, "get_silent_signal_diagnostics", None)
        if not callable(getter):
            raise RuntimeError("counter_getter_unavailable")
        snap = getter() or {}
        if not isinstance(snap, dict):
            snap = {}
        reasons_raw = snap.get("reasons", {}) or {}
        by_engine_raw = snap.get("by_engine", {}) or {}
        reasons = {}
        if isinstance(reasons_raw, dict):
            for key, value in reasons_raw.items():
                count = _signaldiag_int(value)
                if count > 0:
                    reasons[str(key)] = count
        total = _signaldiag_int(snap.get("total"), sum(reasons.values()))
        started = _signaldiag_epoch(snap.get("started_at"))
        status = "OK" if total > 0 else "COLLECTING"
    except Exception as exc:
        return "\n".join([
            "*Signal Diagnostics*",
            "------------------",
            "Status: `UNAVAILABLE`",
            f"Read Error: `{type(exc).__name__}`",
            "Counters are in-process only. Use this command from the live bot after reload.",
            "Read-only panel. No counters are reset.",
        ])[:3900]

    near_floor = _signaldiag_int(reasons.get("near_vote_floor"))
    selector_only = _signaldiag_int(reasons.get("selector_contribution_only"))
    shadow_only = _signaldiag_int(reasons.get("shadow_contribution_only"))
    unknown_exc = _signaldiag_int(reasons.get("unknown_exception"))
    vote_rescue_success = _signaldiag_vote_rescue_success_count()

    lines = [
        "*Signal Diagnostics*",
        "------------------",
        "Read-only in-process category counters.",
        f"Status: `{status}`",
        f"Collection Started: `{started}`",
        f"Total Silent Events: `{total}`",
        f"Near-Floor Count: `{near_floor}`",
        f"Vote Rescue Eligible: `{near_floor}`",
        f"Vote Rescue Success: `{vote_rescue_success}`",
        f"Selector-Only Count: `{selector_only}`",
        f"Shadow-Only Count: `{shadow_only}`",
        f"Unknown Exceptions: `{unknown_exc}`",
    ]

    lines.append("")
    lines.append("*Top Reasons*")
    top_reasons = sorted(reasons.items(), key=lambda item: item[1], reverse=True)[:10]
    if top_reasons:
        for name, count in top_reasons:
            lines.append(f"- `{name}`: `{count}`")
    else:
        lines.append("- `COLLECTING`: `0`")

    lines.append("")
    lines.append("*Per-Engine Top Reasons*")
    engine_rows = []
    if isinstance(by_engine_raw, dict):
        for engine, row in by_engine_raw.items():
            if not isinstance(row, dict):
                continue
            row_reasons = row.get("reasons", {}) or {}
            reason_items = []
            if isinstance(row_reasons, dict):
                reason_items = sorted(
                    ((str(k), _signaldiag_int(v)) for k, v in row_reasons.items() if _signaldiag_int(v) > 0),
                    key=lambda item: item[1],
                    reverse=True,
                )
            engine_rows.append((
                str(engine or row.get("engine") or "UNKNOWN").upper(),
                str(row.get("role", "") or "unknown"),
                _signaldiag_int(row.get("total")),
                reason_items[:3],
            ))
    engine_rows.sort(key=lambda item: item[2], reverse=True)
    if engine_rows:
        for engine, role, count, reason_items in engine_rows[:8]:
            reason_text = ", ".join(f"{name}:{value}" for name, value in reason_items) if reason_items else "none"
            lines.append(f"- `{engine}` `{role}` total `{count}` | {reason_text}")
    else:
        lines.append("- `COLLECTING`: no per-engine counters yet")

    lines.extend(_signaldiag_spread_liquidity_lines())

    lines.append("")
    lines.append("No settings, counters, strategies, or orders are changed by this panel.")
    return "\n".join(lines)[:3900]


def kb_diagnostics(settings) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔄 Refresh", callback_data="s:diagnostics"),
         InlineKeyboardButton("🚦 Why No Trades", callback_data="diag:no_trades")],
        [InlineKeyboardButton("🧵 Slots", callback_data="diag:slots"),
         InlineKeyboardButton("⛔ Blocks", callback_data="diag:blockers")],
        [InlineKeyboardButton("📐 Strategy", callback_data="diag:strategy"),
         InlineKeyboardButton("🛡 Risk", callback_data="diag:risk")],
        [InlineKeyboardButton("🔬 Micro Sizing", callback_data="diag:micro_sizing")],
        [InlineKeyboardButton("🧪 Shadow Rehab", callback_data="diag:shadow_rehab"),
         InlineKeyboardButton("🧠 IZANAGI Outcomes", callback_data="diag:izanagi_outcomes")],
        [InlineKeyboardButton("🧾 Decision Trace", callback_data="diag:decision_trace"),
         InlineKeyboardButton("📡 API Health", callback_data="diag:api_health")],
        [InlineKeyboardButton("📊 Signal Diagnostics", callback_data="diag:signaldiag")],
        [InlineKeyboardButton("📲 Telegram Health", callback_data="diag:telegram_health")],
        [InlineKeyboardButton("🎛 Control State", callback_data="diag:control_state"),
         InlineKeyboardButton("⚙️ Engines", callback_data="diag:engines")],
        [InlineKeyboardButton("🔧 Overrides", callback_data="diag:overrides"),
         InlineKeyboardButton("🔄 Sync", callback_data="diag:sync")],
        [InlineKeyboardButton("💹 PnL Sync", callback_data="diag:pnl_sync")],
        [InlineKeyboardButton("🛠 Settings", callback_data="settings_menu")],
        [InlineKeyboardButton("🏠 Home", callback_data="main_menu")],
    ])


_ORIG_INLINE_KEYBOARD_BUTTON = InlineKeyboardButton
_ORIG_INLINE_KEYBOARD_MARKUP = InlineKeyboardMarkup
_MOJIBAKE_RUN = re.compile(r"[\u0080-\u00ff]+")
_TEXT_REPLACEMENTS = {
    "? *Diagnostics*": "ðŸ§ª *Diagnostics*",
    "? *Control State*": "ðŸ§­ *Control State*",
    "? *Settings*": "âš™ï¸ *Settings*",
    "? *Settings Â· AI*": "âš™ï¸ *Settings Â· AI*",
    "? *AI*": "ðŸ¤– *AI*",
    "? High-control state active": "âš ï¸ High-control state active",
    "? Unlimited global slots with replacement enabled": "âš ï¸ Unlimited global slots with replacement enabled",
    "? Risk sizing + dynamic sizing active": "âš ï¸ Risk sizing + dynamic sizing active",
    "? Refresh": "ðŸ”„ Refresh",
    "? Why No Trades": "ðŸ§­ Why No Trades",
    "? Slots": "ðŸ§· Slots",
    "? Blocks": "ðŸš« Blocks",
    "? Strategy": "ðŸ“ Strategy",
    "? Risk": "ðŸ›¡ï¸ Risk",
    "? Sync": "ðŸ”„ Sync",
    "? Decision Trace": "ðŸ§¾ Decision Trace",
    "? API Health": "ðŸŒ API Health",
    "? Control State": "ðŸ§­ Control State",
    "? Engines": "ðŸ§  Engines",
    "? Overrides": "âš™ï¸ Overrides",
    "? Settings": "âš™ï¸ Settings",
    "? Home": "ðŸ  Home",
}


def _fix_mojibake(text: str) -> str:
    if not isinstance(text, str):
        return text

    def _decode_run(match: re.Match[str]) -> str:
        chunk = match.group(0)
        try:
            return chunk.encode("latin1").decode("utf-8")
        except Exception:
            return chunk

    fixed = _MOJIBAKE_RUN.sub(_decode_run, text)
    for old, new in _TEXT_REPLACEMENTS.items():
        fixed = fixed.replace(old, new)
    return fixed.replace("Ã‚Â·", "Â·")


_MOJIBAKE_MARKERS = ("Ã", "Â", "â", "ð", "ï")
_TEXT_REPLACEMENTS = {
    "? *Diagnostics*": "🧪 *Diagnostics*",
    "? *Control State*": "🧭 *Control State*",
    "? *Settings*": "⚙️ *Settings*",
    "? *Settings · AI*": "⚙️ *Settings · AI*",
    "? *AI*": "🤖 *AI*",
    "? High-control state active": "⚠️ High-control state active",
    "? Unlimited global slots with replacement enabled": "⚠️ Unlimited global slots with replacement enabled",
    "? Risk sizing + dynamic sizing active": "⚠️ Risk sizing + dynamic sizing active",
    "? Refresh": "🔄 Refresh",
    "? Why No Trades": "🧭 Why No Trades",
    "? Slots": "🧷 Slots",
    "? Blocks": "🚫 Blocks",
    "? Strategy": "📐 Strategy",
    "? Risk": "🛡️ Risk",
    "? Sync": "🔄 Sync",
    "? Decision Trace": "🧾 Decision Trace",
    "? API Health": "🌐 API Health",
    "? Control State": "🧭 Control State",
    "? Engines": "🧠 Engines",
    "? Overrides": "⚙️ Overrides",
    "? Settings": "⚙️ Settings",
    "? Home": "🏠 Home",
}
_CP1252_BYTE_BY_CODEPOINT = {i: i for i in range(256)}
for _b in range(128, 160):
    try:
        _CP1252_BYTE_BY_CODEPOINT[ord(bytes([_b]).decode("cp1252"))] = _b
    except Exception:
        _CP1252_BYTE_BY_CODEPOINT[_b] = _b
_MOJIBAKE_RUN = re.compile(
    r"(?:[ÃÂâðï][\u0080-\u00ff\u0160\u0152\u017d\u0161\u0153\u017e\u0178\u0192\u02c6\u02dc"
    r"\u2010-\u201e\u2020-\u2022\u2026\u2030\u2039\u203a\u20ac\u2122]+)+"
)


def _decode_mojibake_fragment(fragment: str) -> str:
    data = bytearray()
    for ch in fragment:
        b = _CP1252_BYTE_BY_CODEPOINT.get(ord(ch))
        if b is None:
            return fragment
        data.append(b)
    try:
        return data.decode("utf-8")
    except Exception:
        return fragment


def _fix_mojibake(text: str) -> str:
    if not isinstance(text, str):
        return text

    fixed = text
    for _ in range(3):
        if not any(marker in fixed for marker in _MOJIBAKE_MARKERS):
            break
        decoded = _MOJIBAKE_RUN.sub(lambda m: _decode_mojibake_fragment(m.group(0)), fixed)
        if decoded == fixed:
            break
        fixed = decoded

    for old, new in _TEXT_REPLACEMENTS.items():
        fixed = fixed.replace(old, new)
    return fixed.replace("Ã‚Â·", "·").replace("Â·", "·")


def _normalize_markup(markup):
    if markup is None:
        return None
    try:
        rows = getattr(markup, "inline_keyboard", None) or []
    except Exception:
        return markup

    rebuilt = []
    for row in rows:
        new_row = []
        for btn in row:
            try:
                label = _fix_mojibake(getattr(btn, "text", ""))
                kwargs = {}
                for attr in (
                    "callback_data",
                    "url",
                    "web_app",
                    "login_url",
                    "switch_inline_query",
                    "switch_inline_query_current_chat",
                    "callback_game",
                    "pay",
                ):
                    val = getattr(btn, attr, None)
                    if val is not None:
                        kwargs[attr] = val
                new_row.append(_ORIG_INLINE_KEYBOARD_BUTTON(label, **kwargs))
            except Exception:
                new_row.append(btn)
        rebuilt.append(new_row)
    return _ORIG_INLINE_KEYBOARD_MARKUP(rebuilt)


def _wrap_panel_text(fn):
    def _wrapped(*args, **kwargs):
        return _fix_mojibake(fn(*args, **kwargs))
    return _wrapped


def _wrap_keyboard(fn):
    def _wrapped(*args, **kwargs):
        return _normalize_markup(fn(*args, **kwargs))
    return _wrapped


for _name, _obj in list(globals().items()):
    if _name.startswith("panel_") and callable(_obj):
        globals()[_name] = _wrap_panel_text(_obj)
    elif _name.startswith("kb_") and callable(_obj):
        globals()[_name] = _wrap_keyboard(_obj)


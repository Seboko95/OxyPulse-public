"""
OxyPulse — settings_store.py  v7
Runtime settings persisted to data/settings.json.
All changes apply instantly — no restart needed.

v7 changes — Rehab + Automation Reconciliation:
  • Engine operating modes: disabled / shadow / probation / live
  • Engine lanes: trend / breakout / structure / mean-reversion / order-flow
  • Per-engine regime_allowlist, session_allowlist, quality_floor, promotion_state
  • automation_mutation_enabled guard (default False — observe only)
  • Config drift detection on startup
  • Append-only operator change journal (data/change_journal.jsonl)
  • Demotion guard: live → probation → shadow (not straight disable)
  • Shadow tracking for non-live engines
  • Anti-churn: no same-coin re-entry after failed trade within window
"""
from __future__ import annotations
import json
import logging
import os
import time
from pathlib import Path
from copy import deepcopy
from datetime import datetime, timezone

logger = logging.getLogger("OxyPulse.SettingsStore")
_PATH  = Path("data/settings.json")
_JOURNAL_PATH = Path("data/change_journal.jsonl")
_last_load_mtime: float | None = None
_last_hot_reload_check: float = 0.0

# ── Engine operating modes ────────────────────────────────────────────────────
# Each engine has exactly one mode at runtime:
#   disabled  — not loaded, no scanning, no signals
#   shadow    — scans and records hypothetical signals, never places orders
#   probation — places real orders but max_pos=1, tighter thresholds
#   live      — full production mode
VALID_ENGINE_MODES = ("disabled", "shadow", "probation", "live")
VALID_ENGINE_ROLES = ("hunter", "confirmer", "selector", "booster", "specialist", "shadow_advisor")

DEFAULT_ENGINE_ROLES = {
    "HACHIMAN": "selector",
    "HAYATE": "confirmer",
    "KAGUTSUCHI": "confirmer",
    "YATAGARASU": "specialist",
    "RYUJIN": "booster",
    "IZANAGI": "shadow_advisor",
    "SUSANOO": "specialist",
    "FUJIN": "confirmer",
    "AMATERASU": "confirmer",
    "TITAN": "specialist",
    "RONIN": "hunter",
}

ENGINE_ROLE_WEIGHTS = {
    "hunter": 1.00,
    "confirmer": 0.55,
    "selector": 0.45,
    "booster": 0.35,
    "specialist": 0.80,
    "shadow_advisor": 0.15,
}

# ── Engine lane definitions ───────────────────────────────────────────────────
# Each engine belongs to exactly one lane.  Runtime gating checks the lane's
# allowed regimes before the engine evaluates any coin.
ENGINE_LANES = {
    "RYUU":     "trend",
    "RAIJIN":   "trend",
    "HAYATE":   "trend",
    "ARASHI":   "breakout",
    "RONIN":    "breakout",
    "IBUKI":    "breakout",
    "MUSASHI":  "structure",
    "IZANAGI":  "structure",
    "ENMA":     "structure",
    "HACHIMAN": "selector",
    "YATAGARASU": "structure",
    "TENSEI":   "structure",
    "TITAN":    "mean_reversion",
    "KAGE":     "mean_reversion",
    "FUJIN":    "order_flow",
    "KAGUTSUCHI": "order_flow",
    "RYUJIN":   "order_flow",
    "ONRYO":    "order_flow",
    "KANNON":   "protector",
    "GENBU":    "protector",
    "AMATERASU": "trend",       # MTF confluence is trend-aligned
    "SHOGUN":   "structure",
    "TENJIN":   "trend",
    "SUSANOO":  "mean_reversion",
    "OROCHI":   "trend",
    "TSUNAMI":  "breakout",
    "SUIJIN":   "mean_reversion",
    "RAIJUU":   "order_flow",
}

# Which regimes each lane is allowed to trade in
LANE_REGIME_ALLOWLIST = {
    "trend":           ["TRENDING", "VOLATILE"],
    "breakout":        ["TRENDING", "VOLATILE"],
    "structure":       ["TRENDING", "RANGING"],
    "mean_reversion":  ["RANGING"],
    "order_flow":      ["TRENDING", "RANGING", "VOLATILE"],
    "selector":        ["TRENDING", "RANGING", "VOLATILE"],
    "protector":       ["TRENDING", "RANGING", "VOLATILE"],
}

# ── Promotion / demotion thresholds ───────────────────────────────────────────
SHADOW_PROMOTION_THRESHOLDS = {
    "min_trades": 30,
    "min_win_rate": 58.0,
    "min_profit_factor": 1.25,
    "max_api_fail_rate": 2.0,
}
PROBATION_PROMOTION_THRESHOLDS = {
    "min_trades": 10,
    "min_win_rate": 60.0,
    "max_margin_rejects": 2,
}

DEFAULTS: dict = {
    # ── Risk ──────────────────────────────────────────────────────────────────
    # risk_pct is the % of balance risked per trade. At 10x leverage with 2% risk:
    # e.g. $100 balance × 2% = $2 max loss per trade. SL at ~2-3% from entry.
    # Formula: size_usdt = (balance × risk_pct/100) / sl_dist_pct × 100
    # The 10x baseline lowers liquidation risk versus the former 25x profile.
    "risk_sizing_enabled":   True,
    "risk_sizing_min_exec_policy": "skip",
    "adaptive_capital_allocator_enabled": True,
    "adaptive_allocator_use_legacy_min_order_floor": False,
    "adaptive_min_economic_notional_usdt": 1.0,
    "instrument_metadata_ttl_secs": 3600.0,
    "instrument_metadata_refresh_secs": 1800.0,
    # Cooperative engine batches keep Telegram and safety tasks schedulable
    # during large multi-engine scans without changing candidate selection.
    "engine_eval_cooperative_batch_size": 4,
    "capital_reservation_ttl_secs": 120.0,
    "opportunity_queue_enabled": True,
    "opportunity_queue_max_size": 100,
    "opportunity_queue_ttl_secs": 300.0,
    "execution_market_data_max_age_sec": 90.0,
    "opportunity_geometry_ttl_secs": 300.0,
    "opportunity_queue_retry_secs": 10.0,
    "opportunity_reprice_max_pct": 0.50,
    "opportunity_queue_allow_opposing": False,
    "execution_order_timeout_secs": 15.0,
    "execution_retry_count": 1,
    "execution_limit_fallback_enabled": True,
    "execution_cancel_stale_remainder": True,
    "execution_max_economic_spread_pct": 0.30,
    "execution_max_slippage_pct": 0.35,
    "market_filter_allow_unknown_liquidity": False,
    "profitability_control_plane_enabled": True,
    "profitability_prior_strength": 24.0,
    "profitability_uncertain_action": "reduce",
    "operator_timezone": "Africa/Johannesburg",
    "exit_profile_multipliers": {
        "trend": {"be": 1.20, "tp1": 1.10, "tp2": 1.20, "profit_lock": 1.15, "trail_activation": 1.20, "trail_distance": 1.25},
        "breakout": {"be": 1.10, "tp1": 1.00, "tp2": 1.10, "profit_lock": 1.05, "trail_activation": 1.10, "trail_distance": 1.10},
        "mean_reversion": {"be": 0.90, "tp1": 0.85, "tp2": 0.90, "profit_lock": 0.90, "trail_activation": 0.90, "trail_distance": 0.90},
        "order_flow": {"be": 1.00, "tp1": 0.95, "tp2": 1.00, "profit_lock": 0.95, "trail_activation": 1.00, "trail_distance": 1.00},
        "structure": {"be": 1.05, "tp1": 1.00, "tp2": 1.05, "profit_lock": 1.00, "trail_activation": 1.05, "trail_distance": 1.05}
    },
    "execution_short_signal_secs": 30.0,
    "execution_staged_depth_ratio": 0.10,
    "live_taker_fee_rate": 0.0005,
    "live_maker_fee_rate": 0.0002,
    "risk_pct":              1.0,    # % of equity risked per trade; final notional is derived from actual SL distance
    "risk_pct_max":          10.0,   # cap risk multiplier (win streak/profile boost ceiling)
    "atr_sl_multiplier":     2.3,    # 2026-03-27: widened 2.0->2.3x ATR; rolling-20 WR=25%, all sl_exchange exits suggest noise stopping out valid setups
    "atr_tp1_multiplier":    5.0,    # TP1 at ~2:1 RRR with 2.3x ATR SL — wider partial exit
    "atr_tp2_multiplier":    10.0,   # TP2 at ~4:1 RRR — extended runner target for bigger profit
    "min_sl_dist_pct":       1.0,    # floor: SL at least 1% from entry
    "max_sl_dist_pct":       3.0,    # ceiling: SL never >3.0%
    "partial_exit_enabled":  True,
    "trailing_atr_mult":     2.0,    # matches SL multiplier for consistent trailing
    "min_rr_ratio":          3.0,    # 3:1 R:R minimum — ensures real breathing room on TP

    # ── Realized Risk Ceiling (prevents oversized SL realizations) ───────────
    # Caps the expected USDT loss at the current SL distance by reducing size
    # (preferred) and, only if needed to stay executable, tightening the SL.
    # This is designed to reduce large `sl_exchange` tails without starving trade flow.
    "realized_risk_ceiling_enabled": True,
    # Risk cap scales with balance but is bounded.
    "realized_risk_cap_pct_balance": 1.0,   # 1% of balance (e.g. $175 → $1.75)
    "realized_risk_cap_min_usdt":    1.00,
    "realized_risk_cap_max_usdt":    3.00,
    # If required size would fall below executable minimum, tighten SL to satisfy the cap.
    "realized_risk_ceiling_tighten_sl_if_needed": True,
    # Emit sizing/SL cap telemetry to logs + registry metadata.
    "realized_risk_ceiling_telemetry_enabled": True,

    # ── Leverage ──────────────────────────────────────────────────────────────
    "leverage_default":      10,     # safer baseline leverage across the active engine set
    "leverage_min":          5,
    "leverage_max":          15,
    "leverage_high_prob":    15,     # allow higher-conviction trades to use the live cap

    # Vol-adjusted leverage can reduce risk when ATR expands and only lifts within leverage_max.
    "vol_leverage_enabled":  True,
    "vol_leverage_high_atr": 2.0,
    "vol_leverage_low_atr":  0.4,

    # ── Candle timeframe ──────────────────────────────────────────────────────
    "candle_timeframe":      "5m",

    # ── Loss controls ─────────────────────────────────────────────────────────
    "daily_cap_enabled":     True,
    "daily_loss_pct":        8.0,
    "weekly_cap_enabled":    True,
    "weekly_loss_pct":       25.0,

    # ── Weekly Cap (adaptive; NEW decisions only) ─────────────────────────────
    # Replaces blunt weekly hard-blocking with a mode-aware "allow reduced" policy.
    # off | soft | strict | adaptive
    "weekly_cap_mode": "adaptive",
    # Thresholds are expressed as weekly net-loss % of balance (net weekly PnL, losses only).
    # Defaults preserve legacy behavior by starting from weekly_loss_pct.
    "weekly_cap_threshold_pct": 25.0,
    "weekly_cap_recovery_threshold_pct": 15.0,
    "weekly_cap_min_confidence": 4.2,
    # When True and weekly drawdown is beyond the main threshold, only allow
    # clearly high-quality setups (selector/multi-engine/top-ranked) to pass (reduced).
    "weekly_cap_allow_high_conf_only": True,
    "weekly_cap_size_reduction": 0.5,
    "weekly_cap_cooldown_seconds": 120,
    "consec_loss_limit":     3,
    "consec_loss_reduction": 50.0,

    # ── Drawdown ──────────────────────────────────────────────────────────────
    "auto_reduce_dd_pct":    15.0,
    "blackswan_btc_pct":     15.0,

    # ── Win Rate Monitor ─────────────────────────────────────────────────────
    # Background job checks rolling-20 win rate every 30 minutes.
    # Sends a Telegram alert if it drops below the threshold.
    # Sends a recovery alert when it climbs back above.
    "winrate_alert_enabled":    True,
    "winrate_alert_threshold":  40.0,  # % — alert if rolling-20 WR drops below this
    "winrate_alert_min_trades": 10,    # don't alert until at least N trades recorded
    "engine_guard_enabled":     True,  # auto-disable underperforming engines for unattended runs
    "engine_guard_roll_window": 12,
    "engine_guard_min_trades":  8,
    "engine_guard_wr_floor":    40.0,
    "engine_guard_loss_streak": 8,
    "engine_guard_today_pnl_floor": -4.0,

    # ── Vote Score & Signal Quality ───────────────────────────────────────────
    # Minimum vote confidence to take a trade. Reduces low-confidence entries.
    # 2.5 = 80%+ signal agreement; filters marginal 2/3 signals
    "min_vote_score":            3.0,    # keep non-core engines strict by default; active-core overrides below

    # ── Short-Side Rehab (non-blocking) ──────────────────────────────────────
    # Applies mild size reduction + telemetry for shorts in weak/choppy conditions,
    # aimed at improving short expectancy without reducing short trade count.
    "short_rehab_enabled": True,
    "short_rehab_weak_regime_size_mult": 0.90,
    "short_rehab_min_adx_1h": 18.0,

    # ── AMATERASU Macro Context Layer (Daily/Weekly weighting) ───────────────
    # Not a hard filter; used for confidence/sizing hints + runner continuation trust.
    "amaterasu_macro_layer_enabled": True,
    "amaterasu_macro_conf_boost": 0.15,     # additive to signal confidence when aligned
    "amaterasu_macro_conf_penalty": 0.10,   # subtract when macro conflicts (never veto)
    "amaterasu_macro_size_boost": 1.05,     # mild size boost when aligned
    "amaterasu_macro_size_penalty": 0.92,   # mild size trim when conflicting

    # Position sizing by signal confidence — flat 1.0 across all vote levels
    "size_multiplier_votes_3":   1.0,    # 100% for 3/3 confidence
    "size_multiplier_votes_2":   1.0,    # 100% for 2/3 confidence (no penalty for marginal)

    # ── Entry filters (trimmed to 5 hard filters) ─────────────────────────────
    # HTF: only blocks if price is >2% beyond the 200 EMA (tightened from 1%)
    "entry_filter_enabled":  True,
    "htf_filter_enabled":    True,
    # Funding: hard block
    "funding_filter_enabled":True,
    "funding_long_max":      0.001,
    "funding_short_min":    -0.001,
    # OI spike: hard block if OI spikes >20%
    "oi_spike_filter":       True,
    # OI context: now SOFT ONLY (warning in logs, not a block)
    "oi_context_filter_enabled": False,   # downgraded from hard block
    # Session: only blocks true dead zone (00:00–02:00 UTC)
    "session_filter_enabled":    True,
    "session_allow_off_hours":   True,    # now allows Asia session entries
    "session_size_scaling":      True,

    # ── Global position cap ───────────────────────────────────────────────────
    # Hard ceiling on total open trades across ALL engines at any one time.
    "max_open_positions":       15,    # finite global safety cap for new entries
    # Slot control upgrade (hot adjustable):
    # - global_slot_mode:
    #     fixed     -> max_open_positions is the hard cap
    #     auto      -> use capital_manager deployment_max_positions
    "global_slot_mode":         "fixed",         # fixed | auto
    # Trade selection policy (hot adjustable):
    # - first_come       -> current flow: accept in evaluation order until slots full
    # - best_score       -> reserve last slots for higher-quality trades (optional selectivity)
    # - replace_weakest  -> allow slot replacement when full (via slot rotation logic)
    # Default: best-score fill ordering with slot rotation available when full.
    "trade_selection_policy":   "best_score",
    # Engine slot mode default (per-engine can override via engine_<name>_slot_mode):
    # fixed | inherit | unlimited
    "engine_slot_mode":         "fixed",
    "max_same_direction_positions": 4,
    "per_engine_cap_enabled":   True,
    "major_lane_reserved_slots": 1,
    "liquid_alt_universe_limit": 36,
    "major_engine_universe_limit": 20,
    # Replacement / rotation cooldown (prevents rapid churn when slots are full).
    "slot_rotation_cooldown_secs": 240.0,
    # Best-score policy: reserve the last N slots for higher-quality trades.
    # This is intentionally light-touch and only active when trade_selection_policy == best_score.
    "best_score_reserve_slots": 2,
    "best_score_quality_floor": 55.0,

    # ── Winrate optimization layer (ranking/selection only; new decisions only) ──
    # This layer adjusts candidate ranking and slot behavior to improve effective winrate
    # without turning the system into a low-frequency "no-trade" machine.
    "winrate_optimization_enabled": False,
    # balanced | strict | adaptive
    "winrate_profile": "balanced",

    # ── Debug Override / Blocker Probe (diagnostics only; NEW decisions only) ──
    # This system allows the operator to test whether specific blocker families
    # are the true reason trades are being rejected.
    #
    # Modes:
    #   off           -> normal behavior
    #   probe         -> no bypass; annotate blocked candidates with "would pass if" info
    #   live_override -> apply selected relaxations to NEW decisions (open trades untouched)
    "debug_override_mode": "off",  # off | probe | live_override

    # Per-blocker family levels: normal | relaxed | unlimited (bounded/safe equivalents)
    "debug_override_global_cap": "normal",
    "debug_override_engine_cap": "normal",
    "debug_override_weekly_cap": "normal",
    "debug_override_exposure_cap": "normal",
    "debug_override_correlation_cap": "normal",
    "debug_override_same_direction_cap": "normal",
    "debug_override_portfolio_direction_cap": "normal",
    "debug_override_lane_budget": "normal",

    # ── Automation guard ──────────────────────────────────────────────────────
    # When False, the automation (8h scheduled jobs) may REPORT but never
    # mutate code or config.  Must be explicitly re-enabled by operator.
    "automation_mutation_enabled": False,

    # ── Novo Intelligence (advisory-only; disabled by master default) ─────────
    # These modules are pure analysis helpers. The master switch defaults OFF,
    # submodules default ON only so they are ready if the operator enables the
    # master later. They must not hard-block trades or mutate execution state.
    "novo_intelligence_enabled": False,
    "novo_market_dna_enabled": True,
    "novo_market_structure_enabled": True,
    "novo_4h_bias_enabled": True,
    "novo_crt_enabled": True,
    "novo_liquidity_sweep_enabled": True,
    "novo_session_model_enabled": True,
    "novo_trade_quality_enabled": True,
    "novo_ai_trade_memory_enabled": False,
    "novo_gem_hunter_enabled": False,
    "novo_soft_confidence_enabled": False,
    "novo_exit_enrichment_enabled": False,
    "novo_profit_lock_enrichment_enabled": False,
    "novo_fee_bleed_guard_enabled": False,
    "fee_bleed_guard_min_net_profit_usdt": 0.10,
    "fee_bleed_guard_max_fee_share": 0.35,
    "fee_bleed_guard_min_partial_notional_usdt": 10.0,
    "fee_bleed_guard_trace_all_small_exits": True,
    "novo_elite_size_boost_enabled": False,
    "novo_enrichment_outcome_telemetry_enabled": False,
    "novo_trade_quality_advisory_only": True,
    "novo_no_hard_blocks": True,
    "gem_hunter_alert_only": True,
    "positive_mfe_profit_guard_enabled": False,
    "positive_mfe_guard_mfe_roi_trigger": 3.0,
    "positive_mfe_guard_min_profit_usdt": 0.10,
    "positive_mfe_guard_fee_mult": 2.0,
    "positive_mfe_guard_lock_roi_pct": 0.35,
    "positive_mfe_guard_min_age_sec": 30.0,
    "positive_mfe_guard_max_lock_levels": 1,
    "positive_profit_lock_enrichment_enabled": False,
    "profit_lock_enrichment_mfe_trigger_roi": 25.0,
    "profit_lock_enrichment_lock_pct_1": 0.20,
    "profit_lock_enrichment_lock_pct_2": 0.35,
    "profit_lock_enrichment_lock_pct_3": 0.50,
    "profit_lock_enrichment_max_level": 3,

    # ── Category engine toggles ───────────────────────────────────────────────
    # Legacy enabled flags kept for backward compat — runtime uses engine_<name>_mode.
    "engine_ryuu_enabled":      False,
    "engine_raijin_enabled":    False,
    "engine_kage_enabled":      False,
    "engine_arashi_enabled":    False,
    "engine_musashi_enabled":   False,
    "engine_shogun_enabled":    False,
    "engine_tenjin_enabled":    True,
    "engine_izanagi_enabled":   True,
    "engine_susanoo_enabled":   False,
    "engine_fujin_enabled":     False,
    "engine_amaterasu_enabled": True,
    "engine_titan_enabled":     True,
    "engine_ronin_enabled":     True,
    "engine_enma_enabled":      False,
    "engine_hachiman_enabled":  False,
    "engine_hayate_enabled":    False,
    "engine_kagutsuchi_enabled": False,
    "engine_yatagarasu_enabled": False,
    "engine_ryujin_enabled":    False,
    "engine_tensei_enabled":    False,
    "engine_ibuki_enabled":     False,
    "engine_onryo_enabled":     False,
    "engine_kannon_enabled":    False,
    "engine_genbu_enabled":     False,

    # ── Engine operating modes ────────────────────────────────────────────────
    # Values: "disabled" | "shadow" | "probation" | "live"
    # These override the legacy enabled flags at runtime.
    "engine_ryuu_mode":         "shadow",        # shadow until promoted
    "engine_raijin_mode":       "live",
    "engine_kage_mode":         "live",
    "engine_arashi_mode":       "live",
    "engine_musashi_mode":      "live",
    "engine_shogun_mode":       "shadow",       # not ready for live
    "engine_tenjin_mode":       "live",
    "engine_izanagi_mode":      "live",         # AI composite — live
    "engine_susanoo_mode":      "live",
    "engine_fujin_mode":        "live",
    "engine_amaterasu_mode":    "live",         # MTF confluence — primary engine
    "engine_titan_mode":        "live",         # Mean reversion — live
    "engine_ronin_mode":        "live",         # Breakout momentum — live
    "engine_orochi_mode":       "live",
    "engine_tsunami_mode":      "live",
    "engine_suijin_mode":       "live",
    "engine_raijuu_mode":       "live",
    "engine_enma_mode":         "shadow",
    "engine_hachiman_mode":     "shadow",
    "engine_hayate_mode":       "shadow",
    "engine_kagutsuchi_mode":   "shadow",
    "engine_yatagarasu_mode":   "probation",
    "engine_ryujin_mode":       "probation",
    "engine_tensei_mode":       "disabled",
    "engine_ibuki_mode":        "shadow",
    "engine_onryo_mode":        "shadow",
    "engine_kannon_mode":       "shadow",
    "engine_genbu_mode":        "shadow",

    # ── Per-engine quality floors ─────────────────────────────────────────────
    # Minimum quality score from engine evaluate() to accept a signal.
    "engine_ryuu_quality_floor":      0.0,
    "engine_amaterasu_quality_floor": 0.0,
    "engine_izanagi_quality_floor":   0.30,
    "engine_titan_quality_floor":     0.0,
    "engine_ronin_quality_floor":     0.0,

    # ── Per-engine promotion state ────────────────────────────────────────────
    # Tracks shadow/probation trade counts and metrics for promotion decisions.
    "engine_ryuu_shadow_trades":      0,
    "engine_ryuu_shadow_wins":        0,
    "engine_ryuu_shadow_pnl":         0.0,

    # ── Per-engine max simultaneous positions ─────────────────────────────────
    # Prevents any one engine monopolising all slots (RYUU first-mover problem).
    # Each engine checks its own count before opening. Works alongside the
    # global max_open_positions cap — both must pass before a trade is taken.
    "engine_ryuu_max_pos":      5,
    "engine_raijin_max_pos":    5,
    "engine_kage_max_pos":      5,
    "engine_arashi_max_pos":    5,
    "engine_musashi_max_pos":   5,
    "engine_shogun_max_pos":    5,
    "engine_tenjin_max_pos":    2,
    "engine_izanagi_max_pos":   5,
    "engine_susanoo_max_pos":   5,
    "engine_fujin_max_pos":     5,
    "engine_amaterasu_max_pos": 5,
    "engine_titan_max_pos":     5,
    "engine_ronin_max_pos":     5,
    "engine_enma_max_pos":      5,
    "engine_hachiman_max_pos":  1,
    "engine_hayate_max_pos":    5,
    "engine_kagutsuchi_max_pos": 5,
    "engine_yatagarasu_max_pos": 2,
    "engine_ryujin_max_pos":    2,
    "engine_tensei_max_pos":    5,
    "engine_ibuki_max_pos":     5,
    "engine_onryo_max_pos":     5,
    "engine_kannon_max_pos":    1,
    "engine_genbu_max_pos":     1,

    # ── Active core engine vote thresholds ────────────────────────────────────
    # Keep the live core productive without lowering the global bar for dormant engines.
    "ryuu_min_votes":          3.0,
    "amaterasu_min_votes":     3.0,
    "izanagi_min_votes":       3.0,
    "hachiman_min_votes":      2.0,
    "yatagarasu_min_votes":    3.5,
    "ryujin_min_votes":        2.5,

    # ── IZANAGI HTF Elliott Wave rehab settings ───────────────────────────────
    "izanagi_size_mult_3votes":  1.0,
    "izanagi_size_mult_2votes":  1.0,
    "izanagi_ai_confirm_threshold": 0.30,
    "izanagi_allow_nano_route": False,

    # ── TITAN Large-Cap Mean Reversion engine ────────────────────────────────
    "titan_leverage":          15,
    "titan_min_votes":         3.0,   # 3/3 votes required across all engines
    "titan_size_mult_3votes":  1.0,   # flat 100% — consistent sizing across all votes
    "titan_size_mult_2votes":  1.0,   # 100% position on 2/3 vote

    # ── RONIN Large-Cap Breakout Momentum engine ─────────────────────────────
    "ronin_leverage":          15,
    "ronin_size_mult_3votes":  1.0,   # flat 100% sizing
    "ronin_size_mult_2votes":  1.0,   # flat 100% sizing
    "ronin_min_votes":         3.0,
    "ronin_breakout_buffer_pct": 0.15,
    "ronin_volume_confirm_mult": 1.8,
    "ronin_adx_min":           28.0,
    "ronin_adx_rising_min":    0.75,
    "ronin_vwap_buffer_pct":   0.15,
    "ronin_allow_nano_route":  False,

    # ── TENJIN swing engine ───────────────────────────────────────────────────
    "tenjin_sl_pct":         2.5,    # fallback only when ATR is missing
    "tenjin_tp_pct":         9.0,    # fallback only when ATR is missing
    # Swing posture (TENJIN):
    "tenjin_leverage":       8,
    "swing_tp1_trigger_pct": 6.50,
    "swing_tp2_trigger_pct": 12.00,
    "swing_breakeven_trigger_pct": 3.20,
    "swing_breakeven_buffer_pct": 0.40,
    "swing_profit_lock_trigger_pct": 6.80,
    "swing_profit_lock_buffer_pct": 2.00,
    "swing_trailing_activation_pct": 8.50,
    "swing_trailing_distance_pct": 3.20,
    "swing_tp1_close_pct": 25.0,
    "swing_tp2_close_pct": 25.0,
    "swing_max_open_positions": 2,
    "swing_risk_budget_pct": 9.0,

    # ── SUSANOO funding squeeze engine ────────────────────────────────────────
    "susanoo_funding_threshold": 0.0005,  # 0.05% — extreme funding triggers engine

    # ── AMATERASU multi-timeframe engine ─────────────────────────────────────
    "amaterasu_leverage":    15,

    # ── Open Interest minimum ─────────────────────────────────────────────────
    "oi_min_usd":            500_000,  # skip coins with OI < $500k (thin market)

    # ── Minimum order size ────────────────────────────────────────────────────
    # At 10x leverage: $100 notional = $10 margin per trade.
    # Floor ensures every trade is at least $4 margin regardless of balance.
    "min_order_usdt":        100.0,
    "adaptive_min_order_floor_enabled": True,
    "contract_aware_execution_floor_enabled": True,
    "min_order_usdt_micro":  25.0,
    "min_order_usdt_growth": 50.0,
    "min_order_usdt_pro":    100.0,
    "micro_sizing_enabled":  True,
    "micro_min_order_usdt_floor": 5.0,
    "micro_margin_buffer_min_usdt": 0.50,
    "micro_margin_buffer_pct_balance": 12.0,
    "micro_margin_buffer_max_usdt": 5.0,

    # ── Percentage-based position sizing ─────────────────────────────────────
    # Formula: max(position_size_usdt, balance × position_size_pct)
    # then hard-clamped to [floor, cap] before vote-score multipliers apply.
    #   $50 bal  → max(100, 25.0)  = $100 (floor dominates)  → $4 margin
    #   $250 bal → max(100, 125.0) = $125                     → $5 margin
    #   $500 bal → max(100, 250.0) = $250 (cap hit)           → $10 margin
    "position_size_pct":     0.5,
    "position_size_usdt":    100.0,
    "position_size_cap_pct_balance": 100.0,
    "position_size_cap_usdt": 250.0,
    # Balance buffer: keep at least $5 free before allowing new entries.
    # Lowered from $40 so the bot can trade with $40 starting balance.
    "min_free_balance_buffer_usdt": 5.0,
    "preflight_contract_cap_enabled": True,
    "preflight_max_reduce_enabled": True,
    "leverage_fallback_enabled": True,
    "preflight_cooldown_enabled": True,
    "preflight_zero_contract_window_secs": 1800.0,
    "preflight_zero_contract_trigger_count": 2,
    "preflight_zero_contract_cooldown_secs": 1800.0,
    "preflight_zero_contract_selection_penalty": 40.0,
    "execution_failure_penalty_window_secs": 3600.0,
    "exchange_admission_selection_penalty": 12.0,
    "execution_cooldown_selection_penalty": 1000.0,
    "sync_health_degraded_failures": 1,
    "sync_health_down_failures": 3,

    # ── Cooldown ──────────────────────────────────────────────────────────────
    "cooldown_enabled":      True,
    "cooldown_mins":         10,
    # Dynamic cooldown: after win → 5min (ride momentum), after loss → 20min (wait)
    "dynamic_cooldown_enabled": True,

    # ── Daily profit target ───────────────────────────────────────────────────
    "daily_profit_target_enabled": False,
    "daily_profit_target_pct":     5.0,

    # ── Trailing Stop Loss ────────────────────────────────────────────────────
    "tsl_enabled":           True,
    "tsl_activation_pct":    3.0,
    "tsl_trail_pct":         1.10,
    "trail_activation_profit_usd": 0.90,
    "trail_distance_mode":   "hybrid",  # percent | atr | hybrid
    "trail_distance_atr_mult": 2.5,

    # ── Breakeven ─────────────────────────────────────────────────────────────
    "breakeven_enabled":     True,
    "breakeven_to_tp1":      False,   # legacy fallback only; secure-profit mode uses fee-aware breakeven
    "breakeven_buffer_pct":  0.18,
    "breakeven_activation_profit_usd": 0.35,
    "breakeven_fee_buffer_mult": 2.0,

    # ── Stale Trade Exit ──────────────────────────────────────────────────────
    # Close trades that haven't reached TP1 within N minutes and are flat (<1.5% move).
    # Frees margin from dead trades before they eventually hit SL.
    "stale_trade_exit_enabled": True,
    "stale_trade_mins":         18,    # minutes — stale exit guard (kept conservative; avoid killing good trades)
    # Stale exit behavior controls (affects NEW exit decisions only).
    "stale_exit_require_nonpositive": True,
    "stale_exit_block_if_tp1_hit": True,
    "stale_exit_block_if_profit_lock_active": True,

    # ── SL Wick Guard ─────────────────────────────────────────────────────────
    "sl_timeout_enabled":    True,
    "sl_timeout_secs":       8,
    "internal_sl_confirm_secs": 12,

    # ── 3-5-7 Rule ────────────────────────────────────────────────────────────
    "rule_357_enabled":          True,
    "rule_357_max_open_risk_pct":12.0,
    "rule_357_daily_halt_pct":   7.0,

    # ── Soft accept system ────────────────────────────────────────────────────
    # Allows trades that fail exactly one SOFT filter (HTF, funding, OI min, session)
    # when confidence is high enough.  Hard filters (blackswan, weekly cap, 3-5-7,
    # exposure, drawdown, correlation) are NEVER bypassed.
    # Soft-accepted trades use a reduced position size to compensate for the
    # lower entry quality.  Tagged as "soft_accept" in rejection_log.
    "engine_autonomy_enabled":        True,
    "confirm_path_enabled":           True,
    "autonomy_same_coin_confirm_enabled": True,
    "autonomy_same_coin_scalein_enabled": False,
    "autonomy_same_coin_max_adds":    0,
    "autonomy_same_coin_scalein_mult": 0.0,
    "duplicate_merge_window_sec":     90,
    "role_contribution_min_votes":    2.0,
    "vote_rescue_enabled":            False,
    "vote_rescue_max_vote_gap":       1.0,
    "vote_rescue_min_quality":        62.0,
    "vote_rescue_min_admission_confidence": 2.0,
    "vote_rescue_require_support":    True,
    "vote_rescue_novo_min_tier":      "GOOD",
    "vote_rescue_live_opener_roles":  "hunter,specialist,confirmer,booster",
    "engine_scan_timeout_sec":        180.0,
    "positive_net_shadow_rehab_enabled": True,
    "positive_net_shadow_rehab_engines": "RONIN,RYUJIN",

    # Shadow rehab telemetry is read-only infrastructure. These defaults never
    # promote, demote, resize, or alter execution without a future explicit change.
    "shadow_rehab_enabled": True,
    "shadow_rehab_auto_actions_enabled": False,
    "shadow_rehab_probation_enabled": False,
    "shadow_rehab_engines": "FUJIN,HAYATE,KAGUTSUCHI,RYUJIN,RYUU,AMATERASU",
    "role_confirmer_solo_quality":    78.0,
    "role_specialist_quality":        72.0,
    "role_positive_engine_quality":   68.0,
    "position_strength_size_boost_enabled": True,
    "position_strength_size_boost_max": 0.10,
    "strict_pnl_accuracy_enabled": True,
    "position_sync_grace_sec": 3,
    "live_position_refresh_sec": 3,
    "live_position_refresh_jitter_sec": 0.5,
    "live_position_retry_sec": 1.5,
    "max_live_position_stale_sec": 10,
    "pnl_drift_warn_pct": 10.0,
    "pnl_drift_warn_usd": 0.05,
    "pnl_desync_jump_pct": 50.0,
    "pnl_desync_hold_sec": 3,
    "pnl_okx_truth_lock_sec": 4,
    "same_direction_headroom_enabled": True,
    "lane_budget_enabled":            True,
    "per_lane_budget_enabled":        True,   # backward-compatible alias for lane_budget_enabled
    "lane_budget_micro_trend_pct":    80.0,
    "lane_budget_micro_breakout_pct": 80.0,
    "lane_budget_micro_structure_pct": 80.0,
    "lane_budget_micro_mean_reversion_pct": 80.0,
    "lane_budget_micro_order_flow_pct": 70.0,
    "lane_budget_growth_trend_pct":   45.0,
    "lane_budget_growth_breakout_pct": 40.0,
    "lane_budget_growth_structure_pct": 40.0,
    "lane_budget_growth_mean_reversion_pct": 35.0,
    "lane_budget_growth_order_flow_pct": 35.0,
    "lane_budget_pro_trend_pct":      30.0,
    "lane_budget_pro_breakout_pct":   25.0,
    "lane_budget_pro_structure_pct":  25.0,
    "lane_budget_pro_mean_reversion_pct": 20.0,
    "lane_budget_pro_order_flow_pct": 20.0,
    "lane_override_enabled":      True,   # MICRO-only guarded override path; ignored outside MICRO
    "lane_override_min_confidence": 2.7,
    "lane_override_active_session_min_confidence": 2.3,
    "soft_accept_enabled":         True,
    "soft_accept_max_soft_failures": 1,
    "soft_accept_min_confidence":  3.5,   # min votes / confidence_score to trigger
    "soft_accept_min_confidence_micro":  2.5,
    "soft_accept_min_confidence_growth": 3.3,
    "soft_accept_min_confidence_pro":    3.5,
    "soft_accept_off_session_confidence_boost_micro": 0.2,
    "soft_accept_size_mult":       0.65,  # position size multiplier (65% of normal)
    "soft_accept_size_multiplier_micro":  0.80,
    "soft_accept_size_multiplier_growth": 0.65,
    "soft_accept_size_multiplier_pro":    0.55,
    "soft_accept_lane_budget_bonus_micro": 10.0,
    "soft_accept_lane_budget_bonus_micro_off": 5.0,
    "blocked_candidate_cooldown_secs": 180.0,
    "blocked_candidate_suppress_repeats": True,
    "blocked_candidate_group_window_secs": 300.0,
    "full_universe_candidate_limit": 0,
    "engine_candidate_limit_default": 18,
    "engine_candidate_limit_hachiman": 10,
    "engine_candidate_limit_hayate": 14,
    "engine_candidate_limit_kagutsuchi": 14,
    "engine_candidate_limit_ryujin": 14,
    "candidate_quality_summary_enabled": True,
    "universe_min_volume_usd_24h": 250000.0,
    "universe_spread_max_pct": 1.0,
    "spread_filter_enabled": True,
    "dead_coin_suppression_enabled": True,
    "executability_penalty_enabled": True,
    "market_cap_bonus_enabled": True,
    "small_account_large_cap_preference_enabled": True,
    "large_cap_preference_balance_usdt": 100.0,
    "large_cap_preference_bonus_small_account": 8.0,
    "large_cap_preference_bonus_default": 3.0,
    "headroom_scaling_enabled": True,
    "block_diagnostics_enabled": True,
    "engine_consensus_enabled": True,
    "engine_consensus_window_secs": 900.0,
    "engine_consensus_bonus_per_engine": 8.0,
    "engine_consensus_max_bonus": 24.0,
    "role_consensus_conf_lift_enabled": True,
    "role_consensus_conf_lift_min_count": 3,
    "role_consensus_conf_lift_min_strength": 5.5,
    "role_consensus_conf_lift_min_quality": 65.0,
    "role_consensus_conf_lift_max": 1.25,

    # ── Intelligence engine pack (safe defaults) ────────────────────────────
    "enma_top_pick_limit": 1,
    "hayate_short_edge_min": 0.003,
    "hayate_medium_edge_min": 0.006,
    "hayate_long_edge_min": 0.010,
    "kagutsuchi_imbalance_long": 0.62,
    "kagutsuchi_imbalance_short": 0.38,
    "kagutsuchi_displacement_atr_mult": 0.60,
    "tensei_reentry_window_mins": 240,
    "tensei_reentry_cooldown_mins": 45,
    "safe_reentry_enabled": True,
    "tensei_reentry_profit_only": True,
    "ibuki_bbw_compression_mult": 0.75,
    "ibuki_atr_compression_mult": 0.80,
    "ibuki_breakout_range_mult": 1.20,
    "onryo_funding_extreme": 0.0006,
    "onryo_oi_delta_min_pct": 0.03,
    "onryo_displacement_atr_mult": 0.70,

    # ── Profit capture / secure-profit model ─────────────────────────────────
    "tp_distance_mode": "extended",  # conservative | balanced | extended
    # TP placement / visibility model:
    # off            -> no TP reference shown, no exchange TP attached
    # off            -> no profit-taking TP owner; stop/BE/trailing remain active
    # reference_only -> local staged TP1/TP2 owner when partial exits are enabled
    # exchange_tp    -> exchange-attached full-position TP owner
    # hybrid         -> local staged TP1/TP2 owner when partial exits are enabled;
    #                   exchange-side SL remains crash-safe and no competing full TP is attached
    "tp_mode": "hybrid",
    "profit_target_enabled": True,
    "min_profit_target_usdt": 2.0,
    "profit_target_max_widen_mult": 1.75,
    "profit_target_skip_unsafe": True,
    "secure_profit_mode_enabled": True,
    "secure_profit_target_usd": 3.0,
    "secure_profit_activation_multiple": 0.60,
    # Flow exit profile (leveraged ROI% triggers; NEW exits only).
    # These values are tuned to avoid micro-profit churn while securing profit early.
    "tp1_trigger_pct": 1.80,
    "tp2_trigger_pct": 3.80,
    "breakeven_trigger_pct": 1.10,
    "profit_lock_trigger_pct": 2.20,
    "profit_lock_buffer_pct": 0.65,   # ROI% to lock in (converted to price delta by leverage)
    "trailing_activation_pct": 3.00,
    "trailing_distance_pct": 1.10,
    "partial_tp1_close_pct": 25.0,
    "partial_tp2_close_pct": 20.0,
    "partial_tp1_max_close_pct": 30.0,
    "partial_tp2_max_close_pct": 25.0,
    "runner_core_min_remaining_pct": 45.0,
    "continuation_runner_core_min_pct": 55.0,
    "trailing_after_tp1_pct": 0.85,   # ROI% trailing distance after TP1
    "trailing_after_tp2_pct": 0.60,   # ROI% trailing distance after TP2
    "min_net_take_profit_usdt": 0.16,     # skip partial TP if below this (fees-aware floor)
    # Historical activation/telemetry floor.  It is not a terminal optional-close
    # net floor; lifecycle economics use current costs and the fee multiple below.
    "min_full_close_profit_usdt": 0.12,
    "minimum_net_capture_fee_multiple": 1.0,  # net outcome must cover this multiple of current estimated costs
    "breakeven_secure_fee_mult": 1.20,
    "breakeven_secure_floor_mult": 0.50,
    "partial_skip_log_cooldown_sec": 60.0,
    "profitability_telemetry_enabled": True,
    "fee_aware_partial_deferral_enabled": True,
    "economic_min_runner_notional_usdt": 5.0,
    "economic_min_fee_mult": 8.0,
    "adaptive_exit_enabled": False,
    "runner_quality_enabled": True,
    "symbol_memory_enabled": False,
    "engine_expectancy_shadow_enabled": True,
    "exhaustion_tighten_score_threshold": 3.0,
    "exhaustion_tighten_trail_mult": 0.72,
    "exhaustion_assisted_tightening_enabled": False,
    "continuation_runner_enabled": True,
    "continuation_runner_min_roi": 2.50,
    "continuation_runner_score_threshold": 3.0,
    "continuation_runner_tp2_close_mult": 0.65,
    "continuation_runner_trail_mult": 1.35,
    "weekly_cap_degraded_conf_grace": 0.50,
    "repeat_loss_guard_enabled": True,
    "repeat_loss_guard_window_mins": 720,
    "repeat_loss_guard_min_losses": 2,
    "repeat_loss_guard_extra_conf": 0.60,
    "symbol_instability_size_mult": 0.75,
    "early_invalidation_enabled": True,
    "adaptive_early_invalidation_enabled": False,
    "early_invalidation_min_age_sec": 45.0,
    "early_invalidation_max_age_min": 12.0,
    "early_invalidation_roi_pct": 1.80,
    "early_invalidation_confirm_secs": 8.0,
    "early_invalidation_base_min_hold_sec": 45.0,
    "early_invalidation_min_hold_floor_sec": 20.0,
    "early_invalidation_max_hold_cap_sec": 720.0,
    "early_invalidation_failure_score_threshold": 2.5,
    "early_invalidation_patience_score_threshold": 1.5,
    "early_invalidation_near_tp_grace_enabled": True,
    "early_invalidation_near_tp_floor_ratio": 0.85,
    "early_invalidation_near_tp_grace_sec": 90.0,
    "early_invalidation_chop_strictness_mult": 1.25,
    "early_invalidation_trend_patience_mult": 1.35,
    "winner_patience_mfe_roi_floor": 0.65,
    "winner_patience_runner_quality_floor": 3.0,
    "winner_patience_consensus_floor": 4.0,
    "symbol_direction_loss_cooldown_enabled": True,
    "symbol_direction_loss_threshold": 3,
    "symbol_direction_mild_loss_threshold": 2,
    "symbol_direction_confirm_loss_threshold": 3,
    "symbol_direction_cooldown_loss_threshold": 5,
    "symbol_direction_cooldown_minutes": 0.0,
    "symbol_direction_mild_conf_bump": 0.25,
    "symbol_direction_loss_conf_bump": 0.60,
    "symbol_direction_min_consensus": 2,
    "symbol_direction_quality_override": 82.0,
    "symbol_direction_loss_size_mult": 0.85,
    "symbol_direction_mild_size_mult": 0.90,
    "symbol_direction_memory_decay_hours": 24.0,
    "net_expectancy_gate_enabled": True,
    "net_expectancy_min_cost_mult": 2.0,
    "net_expectancy_weak_cost_mult": 3.0,
    "net_expectancy_soft_size_mult": 0.75,
    "net_expectancy_strong_quality_floor": 82.0,
    "net_expectancy_default_target_move_pct": 0.55,
    "net_expectancy_default_slippage_pct": 0.03,
    "net_expectancy_degraded_penalty_enabled": True,
    "micro_meaningful_profit_enabled": True,
    "micro_meaningful_profit_close_pct": 35.0,
    "micro_meaningful_profit_max_close_pct": 55.0,
    "micro_meaningful_profit_runner_floor_pct": 40.0,
    "micro_meaningful_profit_quality_floor": 3.0,
    "profit_lock_stage1_profit_usd": 0.75,
    "profit_lock_stage1_lock_pct": 20.0,
    "profit_lock_stage2_profit_usd": 1.50,
    "profit_lock_stage2_lock_pct": 40.0,
    "profit_lock_stage3_profit_usd": 3.00,
    "profit_lock_stage3_lock_pct": 60.0,

    # ── Profit lock trail stop (progressive 3-level) ─────────────────────────
    # L1 fires at 15% leveraged profit → SL moves to lock in 5%.
    # L2 fires at 25% leveraged profit → SL moves to lock in 10%.
    # L3 fires at 40% leveraged profit → SL moves to lock in 20%.
    # Each level fires once per trade; SL only ever moves forward (never back).
    # Leveraged % -> actual price % at 10x: 15% = 1.50%, 25% = 2.50%, 40% = 4.00%.
    "profit_lock_enabled":         True,
    "profit_lock_l1_trigger_pct":  15.0,  # L1: trigger at 15% leveraged profit
    "profit_lock_l1_floor_pct":    5.0,   # L1: lock in 5% leveraged profit
    "profit_lock_l2_trigger_pct":  25.0,  # L2: trigger at 25% leveraged profit
    "profit_lock_l2_floor_pct":    10.0,  # L2: lock in 10% leveraged profit
    "profit_lock_l3_trigger_pct":  40.0,  # L3: trigger at 40% leveraged profit
    "profit_lock_l3_floor_pct":    20.0,  # L3: lock in 20% leveraged profit

    # ── Liq heatmap ───────────────────────────────────────────────────────────
    "liq_sl_check_enabled":      True,
    "liq_sl_hard_block":         False,
    "liq_tp_magnet_enabled":     True,
    "liq_tp_magnet_min_dist_pct":0.5,
    "liq_tp_magnet_max_dist_pct":8.0,

    # ── Fee-aware TP ──────────────────────────────────────────────────────────
    "fee_aware_tp_enabled":  True,
    "fee_breakeven_min_rr":  3.0,

    # ── Auto-compounding ──────────────────────────────────────────────────────
    "compounding_enabled":   True,
    "profit_reinvest_pct":   100.0,

    # ── Martingale — OFF by default (conflicts with ATR sizing) ───────────────
    "martingale_enabled":       False,
    "martingale_multiplier":    1.5,
    "martingale_max_levels":    3,
    "martingale_max_risk_pct":  5.0,
    "martingale_halt_loss_pct": 8.0,
    "anti_martingale_enabled":  False,

    # ── Win streak boost — ON: compounds size during hot streaks ─────────────
    "win_streak_boost_enabled": True,   # was False — now enabled for compounding
    "win_streak_threshold":     3,
    "win_streak_boost_pct":     20.0,

    # ── Funding arb ──────────────────────────────────────────────────────────
    "funding_arb_enabled":      False,
    "funding_arb_min_rate":     0.05,
    "funding_arb_capital_pct":  20.0,
    "funding_arb_max_pos":      2,

    # ── OI alert ──────────────────────────────────────────────────────────────
    "oi_alert_spike_pct":    15.0,

    # ── Per-tier sizing legacy ────────────────────────────────────────────────
    # sl_pct / tp_pct are fallback only when ATR is unavailable.
    # min_sl_dist_pct (20%) enforced in risk_manager.calculate_stop_take as hard floor.
    "tiers": {
        "NANO":     {"trade_pct": 70.0, "min_trade_usdt": 6.0, "sl_pct": 20.0, "tp_pct": 30.0, "max_positions": 9999, "daily_loss_pct": 8.0},
        "MICRO":    {"trade_pct": 70.0, "min_trade_usdt": 6.0, "sl_pct": 20.0, "tp_pct": 30.0, "max_positions": 9999, "daily_loss_pct": 8.0},
        "SMALL":    {"trade_pct": 35.0, "min_trade_usdt": 6.0, "sl_pct": 20.0, "tp_pct": 30.0, "max_positions": 9999, "daily_loss_pct": 8.0},
        "STANDARD": {"trade_pct": 15.0, "min_trade_usdt": 6.0, "sl_pct": 20.0, "tp_pct": 30.0, "max_positions": 9999, "daily_loss_pct": 8.0},
    },


    # ── OROCHI: Liquid Trend Pullback engine ──────────────────────────────────
    "engine_orochi_enabled":           False,
    "engine_orochi_max_pos":           2,
    "orochi_min_votes":                3.0,      # rehab mode — all 3 continuation layers must align
    "orochi_leverage":                 15,
    "orochi_size_mult_3votes":         1.0,
    "orochi_size_mult_2votes":         1.0,
    "orochi_pullback_buffer_pct":      0.45,
    "orochi_volume_confirm_mult":      1.1,
    "orochi_funding_abs_max":          0.0008,   # funding only vetoes crowded continuation entries
    "orochi_allow_nano_route":         False,
    "shogun_universe_limit":           12,
    "fujin_universe_limit":            12,

    # ── TSUNAMI: Liquidation Cascade Engine ────────────────────────────────────
    "engine_tsunami_enabled":          False,
    "engine_tsunami_max_pos":          1,
    "tsunami_min_votes":               2.5,      # weighted cascade engine; threshold matches max score
    "tsunami_liq_threshold_usd":       50000.0,  # $50k liq in 5min triggers signal

    # ── SUIJIN: VWAP Reversion Engine (RANGING only) ───────────────────────────
    "engine_suijin_enabled":           False,
    "engine_suijin_max_pos":           1,
    "suijin_min_votes":                1.5,      # weighted reversion signal; regime gate supplies extra quality
    "suijin_vwap_deviation_pct":       1.5,      # price must be 1.5% from VWAP

    # ── RAIJUU: Open Interest Divergence Engine ─────────────────────────────────
    "engine_raijuu_enabled":           False,
    "engine_raijuu_max_pos":           1,
    "raijuu_min_votes":                1.5,      # weighted divergence signal; threshold matches max score
    "raijuu_oi_change_threshold":      0.05,     # 5% OI change triggers check

    # ── Tier 2: MTF Confluence Filter ────────────────────────────���─────────────
    # Checks EMA9 vs EMA21 on 1m/15m/1H; subtracts 1.0 vote if 2+ disagree.
    "mtf_confluence_enabled":          True,

    # ── Correlation Filter ───────────────────────────────────────────────────
    # Blocks opening a new position when 2+ correlated coins are already open
    # in the same direction. Prevents concentrated exposure to one sector.
    "correlation_filter_enabled":      True,
    "correlation_max_same_group":      2,       # max open trades in same group+direction

    # ── Drawdown Recovery Mode ────────────────────────────────────────────────
    # Auto-tightens risk when balance drops 10%+ from peak.
    # Halves position sizes, requires 3/3 votes, limits to top-5 liquid coins.
    "drawdown_recovery_enabled":       False,
    "drawdown_recovery_trigger_pct":   10.0,    # % drop from peak to activate
    "drawdown_recovery_exit_pct":      5.0,     # % drop from peak to deactivate (recovered halfway)

    # ── Daily Recap Notification ──────────────────────────────────────────────
    # Sends a Telegram summary at midnight UTC: W/L, PnL, WR, balance change.
    "daily_recap_enabled":             True,

    # ── Tier 2: Liquidity Heatmap SL Adjustment ────────────────────────────────
    # Widens SL by 0.3% when proposed SL falls inside a dense liq cluster.
    "liq_heatmap_sl_enabled":          True,

    # ── Growth Phase Auto-Promotion ───────────────────────────────────────────
    # The bot auto-adjusts its posture as balance grows through phases:
    #   Phase 1 ($40-$199):   conservative sizing
    #   Phase 2 ($200-$499):  wider slot budget
    #   Phase 3 ($500+):      higher deployment budget
    "growth_phase_auto":               False,
    "growth_phase_2_balance":          200.0,
    "growth_phase_3_balance":          500.0,

    # ═══════════════════════════════════════════════════════════════════════════
    # V6.1 FEATURE FLAGS
    # ═══════════════════════════════════════════════════════════════════════════
    "v6_enabled":                      True,   # master V6 gate
    "v6_weighted_confidence_enabled":  True,   # weighted confidence scoring
    "v6_dynamic_sizing_enabled":       True,   # confidence-adjusted position sizing
    "v6_anti_drawdown_enabled":        True,   # drawdown guard (blocks/reduces on loss regime)
    "v6_smart_execution_enabled":      True,   # execution routing abstraction
    "engine_weight_adaptation_enabled": False,
    "v6_engine_adaptation_enabled":    False,  # adaptive weights (enable after enough trade history)
    "v6_telemetry_enabled":            True,   # structured V6 decision logging (safe ON)

    # ── V6 Confidence thresholds ──────────────────────────────────────────────
    "v6_min_long_confidence":          1.8,    # weighted score needed to approve LONG
    "v6_min_short_confidence":         1.8,    # weighted score needed to approve SHORT
    "v6_max_opposition_score":         0.35,   # opposing-direction weight fraction to block
    "v6_min_supporting_engines":       2,      # min engines that must agree (not used yet)

    # ── V6 Dynamic sizing ─────────────────────────────────────────────────────
    "dynamic_sizing_enabled": True,
    "v6_base_position_size_usdt":      100.0,  # base notional aligned with the V5 floor
    "v6_min_position_size_usdt":       25.0,   # hard floor after all penalties
    "v6_max_position_size_usdt":       250.0,  # hard cap after all multipliers
    "size_mode":                       "blended",  # notional | balance | risk | blended
    "v6_confidence_size_multiplier":   1.0,    # scale factor for confidence boost (1.0 = no extra)
    "v6_volatility_size_penalty":      1.0,    # scale factor for ATR penalty (1.0 = no penalty)
    "v6_drawdown_size_penalty":        1.0,    # scale factor for drawdown (1.0 = no penalty)
    "v6_max_position_pct_of_balance":  0.05,   # max 5% of balance as margin per trade
    "dynamic_target_min_usdt":         50.0,
    "dynamic_target_max_usdt":         100.0,
    "micro_target_min_pct_balance":    300.0,
    "micro_target_max_pct_balance":    900.0,
    "risk_floor_preserve_enabled":     True,   # keep risk-reduced trades executable when the active mode allows it
    "risk_floor_preserve_modes":       "aggressive,full_deployment,max_deploy,single_shot,ai,ai_adaptive",
    "pre_reserve_size_validation_enabled": True,  # reject known non-executable trades before registry reserve
    "risk_auto_reduce_factor":         0.50,   # active loss-recovery multiplier when size reduction is engaged

    # ── V6 Drawdown levels ────────────────────────────────────────────────────
    "v6_drawdown_level1_pct":          3.0,    # level 1: reduce size 20%
    "v6_drawdown_level2_pct":          5.0,    # level 2: reduce size 50%, reduce leverage
    "v6_drawdown_level3_pct":          8.0,    # level 3: restrict to best setups only
    "v6_drawdown_level4_pct":          10.0,   # level 4: pause all new entries

    # ── V6 Execution router ────────────────────────────────────────────────────
    "v6_execution_mode":               "market",   # market | limit | staged
    "v6_execution_allow_limit":        False,       # enable limit order routing
    "v6_execution_allow_staged":       False,       # enable staged/laddered entries

    # ── V6 Per-engine default weights (used by confidence & adaptation engines)
    # Each weight multiplies the raw vote score. 1.0 = neutral, >1.0 = trusted more.
    # Bounds: [0.5, 3.0]. Weights persist in data/engine_weights.json.
    "v6_weight_ryuu":          1.0,
    "v6_weight_raijin":        1.0,
    "v6_weight_kage":          1.0,
    "v6_weight_arashi":        1.0,
    "v6_weight_musashi":       1.0,
    "v6_weight_shogun":        1.0,
    "v6_weight_tenjin":        1.0,
    "v6_weight_izanagi":       1.0,
    "v6_weight_susanoo":       1.0,
    "v6_weight_fujin":         1.0,
    "v6_weight_amaterasu":     1.0,
    "v6_weight_titan":         1.0,
    "v6_weight_ronin":         1.0,
    "v6_weight_orochi":        1.0,
    "v6_weight_tsunami":       1.0,
    "v6_weight_suijin":        1.0,
    "v6_weight_raijuu":        1.0,
    "v6_weight_enma":          1.0,
    "v6_weight_hachiman":      1.0,
    "v6_weight_hayate":        1.0,
    "v6_weight_kagutsuchi":    1.0,
    "v6_weight_yatagarasu":    1.15,
    "v6_weight_ryujin":        1.10,
    "v6_weight_tensei":        1.0,
    "v6_weight_ibuki":         1.0,
    "v6_weight_onryo":         1.0,
    "v6_weight_kannon":        1.0,
    "v6_weight_genbu":         1.0,
    "v6_weight_min":           0.5,    # adaptation lower bound
    "v6_weight_max":           3.0,    # adaptation upper bound
    "v6_weight_decay_rate":    0.02,   # per-cycle decay toward 1.0 (prevents runaway drift)
    "v6_weight_update_min_trades": 5,  # min trades before weight is updated

    # ── Engine-quality overlays for the new intelligence layer ───────────────
    "hachiman_selector_enabled":       True,
    "hachiman_volume_quality_min":     1.2,
    "hachiman_spread_quality_min":     0.55,
    "hachiman_selection_bonus":        8.0,
    "hachiman_top_bias_bonus":         6.0,
    "yatagarasu_breakout_buffer_pct":  0.30,
    "yatagarasu_volume_confirm_mult":  1.25,
    "yatagarasu_flow_ratio_long":      0.61,
    "yatagarasu_flow_ratio_short":     0.39,
    "ryujin_flow_ratio_long":          0.64,
    "ryujin_flow_ratio_short":         0.36,
    "ryujin_volume_confirm_mult":      1.20,
    "ryujin_displacement_atr_mult":    0.85,
    "genbu_advisory_enabled":          True,
    "genbu_quality_floor":             42.0,
    "genbu_soft_penalty":              6.0,

    # ═══════════════════════════════════════════════════════════════════════════
    # V6.1 — CAPITAL-ADAPTIVE ENGINE FLAGS
    # ═══════════════════════════════════════════════════════════════════════════
    "v6_capital_adaptive_enabled":     True,   # master capital-mode gate (safe ON — read-only sizing context)
    "adaptive_live_mode_policy_enabled": True,
    "v6_correlation_control_enabled":  True,   # block correlated/stacking exposure
    # Frequency caps are now diagnostics-only by default; slot/selection logic drives throughput.
    "v6_trade_frequency_enabled":      False,  # per-mode trade frequency cap (advisory)
    "v6_cooldown_system_enabled":      True,   # V6 coin cooldown (distinct from V5 cooldown)
    "v6_market_filter_enabled":        True,   # spread/volume/vol-spike pre-trade filter

    # ── Capital profile / deployment mode ───────────────────────────────────
    "capital_profile":                 "AUTO",     # AUTO | MICRO | GROWTH | PRO
    "deployment_mode":                 "Guarded",  # Guarded | Balanced | Max Deploy | Single Shot | Swarm | Recovery | AI Adaptive
    "deployment_target_utilization_pct": 0.0,      # 0 = use deployment-mode default
    "deployment_max_positions":        0,          # 0 = use deployment-mode default
    "deployment_respect_exposure_cap": True,
    "deployment_respect_headroom":     True,
    "deployment_same_side_limit_pct":  0.0,        # 0 = use deployment-mode default
    "deployment_total_exposure_limit_pct": 0.0,    # 0 = use deployment-mode default

    # ── Capital mode thresholds ──────────────────────────────────────────────
    "v6_micro_max_balance":            100.0,  # below this → MICRO mode
    "v6_growth_max_balance":           1000.0, # below this → GROWTH mode; above → PRO

    # ── MICRO mode params ────────────────────────────────────────────────────
    "v6_micro_risk_multiplier":        1.4,    # scale up risk per trade
    "v6_micro_size_multiplier":        1.3,    # boost position size
    "v6_micro_max_trades_per_hour":    8,
    "v6_micro_max_exposure_pct":       130.0,
    "portfolio_same_direction_pct_micro": 130.0,
    "v6_micro_confidence_threshold":   1.5,    # lower bar to enter more trades
    "v6_micro_drawdown_tolerance_pct": 15.0,   # tolerate more fluctuation

    # ── GROWTH mode params ───────────────────────────────────────────────────
    "v6_growth_risk_multiplier":       1.0,
    "v6_growth_size_multiplier":       1.0,
    "v6_growth_max_trades_per_hour":   5,
    "v6_growth_max_exposure_pct":      60.0,
    "portfolio_same_direction_pct_growth": 60.0,
    "v6_growth_confidence_threshold":  1.8,
    "v6_growth_drawdown_tolerance_pct":10.0,

    # ── PRO mode params ──────────────────────────────────────────────────────
    "v6_pro_risk_multiplier":          0.75,   # reduce risk per trade
    "v6_pro_size_multiplier":          0.85,
    "v6_pro_max_trades_per_hour":      3,
    "v6_pro_max_exposure_pct":         40.0,
    "portfolio_same_direction_pct_pro": 40.0,
    "v6_pro_confidence_threshold":     2.2,    # strict — only best setups
    "v6_pro_drawdown_tolerance_pct":   5.0,    # very defensive

    # ── Confidence engine V6.1 multi-factor params ────────────────────────────
    "v6_regime_trend_boost":           0.15,   # bonus confidence when in TRENDING regime
    "v6_regime_ranging_penalty":       0.10,   # confidence reduction in RANGING
    "v6_mtf_align_boost":              0.20,   # bonus when multi-TF EMA agrees
    "v6_opposition_penalty_factor":    0.25,   # penalty per opposing vote unit
    "v6_vol_high_threshold_atr_pct":   0.04,   # ATR/price > 4% = high volatility
    "v6_vol_low_threshold_atr_pct":    0.01,   # ATR/price < 1% = low volatility

    # ── Exposure tracker ─────────────────────────────────────────────────────
    "v6_max_net_exposure_pct":         70.0,   # max net long/short as % of all open notional
    "v6_max_single_direction_usdt":    500.0,  # max total long OR short notional
    "v6_max_correlated_trades":        2,      # max open trades in same market group + direction

    # ── Market filter ────────────────────────────────────────────────────────
    "v6_spread_max_pct":               0.30,   # bid-ask spread > 0.3% → skip
    "v6_min_volume_usd_24h":           2_000_000.0,  # 24h vol < $2M → skip
    "v6_vol_spike_atr_mult":           3.0,    # ATR > 3× 20-period avg ATR → skip

    # ── Trade frequency tracker ──────────────────────────────────────────────
    "v6_max_trades_per_day":           30,     # absolute hard daily cap regardless of mode

    # ── Engine weights V6.1 upgrades ─────────────────────────────────────────
    "v6_weight_profit_factor_alpha":   0.3,    # profit factor contribution to weight update
    "v6_weight_dd_contrib_penalty":    0.1,    # penalty per drawdown contribution unit

    # ── Trade intent / soft kill switch ──────────────────────────────────────
    "trade_intent_enabled":            True,
    "soft_kill_switch_enabled":        True,
    "trade_intent_mode_override":      "",
    "trade_intent_min_sample_trades":  8,
    "trade_intent_aggressive_wr_pct":  50.0,
    "trade_intent_defensive_wr_pct":   40.0,
    "trade_intent_reduce_trades_wr_pct": 35.0,
    "trade_intent_stop_wr_pct":        30.0,
    "trade_intent_drawdown_defensive_pct": 8.0,
    "trade_intent_aggressive_size_mult": 1.15,
    "trade_intent_aggressive_tp_mult": 1.10,
    "trade_intent_aggressive_max_positions_mult": 1.15,
    "trade_intent_normal_size_mult":   1.0,
    "trade_intent_normal_tp_mult":     1.0,
    "trade_intent_normal_max_positions_mult": 1.0,
    "trade_intent_defensive_size_mult": 0.50,
    "trade_intent_defensive_tp_mult":  0.95,
    "trade_intent_defensive_max_positions_mult": 0.50,
    "trade_intent_stopped_size_mult":  0.0,
    "trade_intent_stopped_tp_mult":    1.0,
    "trade_intent_stopped_max_positions_mult": 0.0,
    "trade_intent_aggressive_freq_mult": 1.15,
    "trade_intent_normal_freq_mult":   1.0,
    "trade_intent_defensive_freq_mult": 0.50,
    "trade_intent_stopped_freq_mult":  0.0,
    "exposure_control_mode":           "Strict",
    "slot_rotation_enabled":           True,
    "slot_rotation_min_age_mins":      45.0,
    "slot_rotation_quality_gap":       12.0,
    "slot_rotation_stagnation_band_pct": 0.35,
    # Replacement behavior tuning (NEW decisions only).
    "replace_weakest_enabled":         True,
    "slot_upgrade_margin_score":       0.85,
    "replace_block_if_profit_locked":  True,
    "replace_block_if_roi_pct_above":  2.50,

    # Open-risk split (NEW admission decisions only; used to keep flow trades from crowding out swing).
    # These are bounded budgets, not frequency caps.
    "open_risk_cap_total_pct":         25.0,
    "open_risk_cap_flow_pct":          16.0,
    "open_risk_cap_swing_pct":         9.0,

    # Diagnostics counters (resettable; does not affect live trading directly)
    "metric_tp1_hits":                 0,
    "metric_tp2_hits":                 0,
    "metric_be_hits":                  0,
    "metric_profit_lock_hits":         0,
    "metric_stale_exits":              0,
    "metric_replacements":             0,
    "trading_mode_preset":             "",
}
_current: dict = deepcopy(DEFAULTS)

_BOOL_ALIAS_GROUPS: dict[str, tuple[str, ...]] = {
    "lane_budget_enabled": ("lane_budget_enabled", "per_lane_budget_enabled"),
    "engine_weight_adaptation_enabled": ("engine_weight_adaptation_enabled", "v6_engine_adaptation_enabled"),
    "confirm_path_enabled": ("confirm_path_enabled", "autonomy_same_coin_confirm_enabled"),
    "dynamic_sizing_enabled": ("dynamic_sizing_enabled", "v6_dynamic_sizing_enabled"),
}

_VALUE_ALIAS_GROUPS: dict[str, tuple[str, ...]] = {
    "correlation_max_same_group": ("correlation_max_same_group", "v6_max_correlated_trades"),
    # Keep legacy weekly_loss_pct and new weekly_cap_threshold_pct in sync.
    "weekly_cap_threshold_pct": ("weekly_cap_threshold_pct", "weekly_loss_pct"),
    # Base trade size is shown in multiple UI surfaces; keep both keys hot and truthful.
    "position_size_usdt": ("position_size_usdt", "v6_base_position_size_usdt"),
}

_PARTICIPATION_TOGGLE_KEYS = {
    "lane_budget_enabled",
    "lane_override_enabled",
    "soft_accept_enabled",
    "entry_filter_enabled",
    "htf_filter_enabled",
    "session_filter_enabled",
    "correlation_filter_enabled",
    "same_direction_headroom_enabled",
    "per_engine_cap_enabled",
    "engine_weight_adaptation_enabled",
    "confirm_path_enabled",
    "safe_reentry_enabled",
    "headroom_scaling_enabled",
}

_EXECUTION_TOGGLE_KEYS = {
    "dynamic_sizing_enabled",
    "micro_sizing_enabled",
    "risk_sizing_enabled",
    "risk_floor_preserve_enabled",
    "pre_reserve_size_validation_enabled",
    "leverage_fallback_enabled",
    "preflight_cooldown_enabled",
    "secure_profit_mode_enabled",
    "profit_target_enabled",
    "v6_execution_allow_limit",
    "v6_execution_allow_staged",
    "breakeven_enabled",
    "partial_exit_enabled",
    "tsl_enabled",
    "fee_aware_partial_deferral_enabled",
    "exhaustion_assisted_tightening_enabled",
    "positive_mfe_profit_guard_enabled",
    "adaptive_early_invalidation_enabled",
    "early_invalidation_near_tp_grace_enabled",
    "symbol_direction_loss_cooldown_enabled",
}

_FILTER_TOGGLE_KEYS = {
    "spread_filter_enabled",
    "dead_coin_suppression_enabled",
    "executability_penalty_enabled",
    "market_cap_bonus_enabled",
    "small_account_large_cap_preference_enabled",
}

_AI_TOGGLE_KEYS = {
    "v6_telemetry_enabled",
    "automation_mutation_enabled",
}

_ADVANCED_TOGGLE_KEYS = {
    "v6_enabled",
    "v6_weighted_confidence_enabled",
    "v6_capital_adaptive_enabled",
    "v6_smart_execution_enabled",
    "v6_market_filter_enabled",
    "v6_trade_frequency_enabled",
    "trade_intent_enabled",
    "soft_kill_switch_enabled",
    "block_diagnostics_enabled",
    "engine_consensus_enabled",
    "engine_guard_enabled",
    "slot_rotation_enabled",
    "winrate_optimization_enabled",
    "profitability_telemetry_enabled",
    "adaptive_exit_enabled",
    "runner_quality_enabled",
    "symbol_memory_enabled",
    "engine_expectancy_shadow_enabled",
}

_LIVE_TOGGLE_KEYS = (
    set(_PARTICIPATION_TOGGLE_KEYS)
    | set(_EXECUTION_TOGGLE_KEYS)
    | set(_FILTER_TOGGLE_KEYS)
    | set(_AI_TOGGLE_KEYS)
    | set(_ADVANCED_TOGGLE_KEYS)
)

_HOT_RUNTIME_VALUE_KEYS = {
    "position_size_usdt",
    "v6_base_position_size_usdt",
    "size_mode",
    "position_size_pct",
    "internal_sl_confirm_secs",
    "risk_sizing_min_exec_policy",
    "leverage_default",
    "min_vote_score",
    "risk_pct",
    "daily_loss_pct",
    "weekly_loss_pct",
    "weekly_cap_mode",
    "weekly_cap_threshold_pct",
    "weekly_cap_recovery_threshold_pct",
    "weekly_cap_min_confidence",
    "weekly_cap_allow_high_conf_only",
    "weekly_cap_size_reduction",
    "weekly_cap_cooldown_seconds",
    "max_open_positions",
    "global_slot_mode",
    "trade_selection_policy",
    "engine_slot_mode",
    "rule_357_max_open_risk_pct",
    "v6_min_position_size_usdt",
    "v6_max_position_size_usdt",
    "v6_max_position_pct_of_balance",
    "dynamic_target_min_usdt",
    "dynamic_target_max_usdt",
    "micro_min_order_usdt_floor",
    "micro_margin_buffer_min_usdt",
    "micro_margin_buffer_pct_balance",
    "micro_margin_buffer_max_usdt",
    "micro_target_min_pct_balance",
    "micro_target_max_pct_balance",
    "risk_floor_preserve_modes",
    "risk_auto_reduce_factor",
    "capital_profile",
    "deployment_mode",
    "deployment_target_utilization_pct",
    "deployment_max_positions",
    "deployment_respect_exposure_cap",
    "deployment_respect_headroom",
    "deployment_same_side_limit_pct",
    "deployment_total_exposure_limit_pct",
    "min_profit_target_usdt",
    "profit_target_max_widen_mult",
    "secure_profit_target_usd",
    "breakeven_trigger_pct",
    "profit_lock_trigger_pct",
    "profit_lock_buffer_pct",
    "tp1_trigger_pct",
    "tp2_trigger_pct",
    "trailing_activation_pct",
    "trailing_distance_pct",
    "trailing_after_tp1_pct",
    "trailing_after_tp2_pct",
    "min_net_take_profit_usdt",
    "min_full_close_profit_usdt",
    "minimum_net_capture_fee_multiple",
    "positive_mfe_guard_mfe_roi_trigger",
    "positive_mfe_guard_min_profit_usdt",
    "positive_mfe_guard_fee_mult",
    "positive_mfe_guard_lock_roi_pct",
    "positive_mfe_guard_min_age_sec",
    "positive_mfe_guard_max_lock_levels",
    "partial_tp1_close_pct",
    "partial_tp2_close_pct",
    "preflight_zero_contract_cooldown_secs",
    "preflight_zero_contract_trigger_count",
    "v6_spread_max_pct",
    "v6_min_volume_usd_24h",
    "universe_spread_max_pct",
    "universe_min_volume_usd_24h",
    "tp_distance_mode",
    "tp_mode",
    "exposure_control_mode",
    "slot_rotation_min_age_mins",
    "slot_rotation_quality_gap",
    "slot_rotation_stagnation_band_pct",
    "slot_rotation_cooldown_secs",
    "best_score_reserve_slots",
    "best_score_quality_floor",
    "winrate_profile",
    "debug_override_mode",
    "debug_override_global_cap",
    "debug_override_engine_cap",
    "debug_override_weekly_cap",
    "debug_override_exposure_cap",
    "debug_override_correlation_cap",
    "debug_override_same_direction_cap",
    "debug_override_portfolio_direction_cap",
    "debug_override_lane_budget",
    "v6_micro_max_exposure_pct",
    "v6_growth_max_exposure_pct",
    "v6_pro_max_exposure_pct",
    "portfolio_same_direction_pct_micro",
    "portfolio_same_direction_pct_growth",
    "portfolio_same_direction_pct_pro",
    "engine_consensus_window_secs",
    "engine_consensus_bonus_per_engine",
    "engine_consensus_max_bonus",
    "engine_candidate_limit_default",
    "engine_candidate_limit_hachiman",
    "engine_candidate_limit_hayate",
    "engine_candidate_limit_kagutsuchi",
    "engine_candidate_limit_ryujin",
    "role_consensus_conf_lift_min_count",
    "role_consensus_conf_lift_min_strength",
    "role_consensus_conf_lift_min_quality",
    "role_consensus_conf_lift_max",
    "trade_intent_mode_override",
    "trade_intent_min_sample_trades",
    "trade_intent_aggressive_wr_pct",
    "trade_intent_defensive_wr_pct",
    "trade_intent_reduce_trades_wr_pct",
    "trade_intent_stop_wr_pct",
    "trade_intent_drawdown_defensive_pct",
    "trade_intent_aggressive_size_mult",
    "trade_intent_aggressive_tp_mult",
    "trade_intent_aggressive_max_positions_mult",
    "trade_intent_aggressive_freq_mult",
    "trade_intent_defensive_size_mult",
    "trade_intent_defensive_tp_mult",
    "trade_intent_defensive_max_positions_mult",
    "trade_intent_defensive_freq_mult",
    "trade_intent_normal_size_mult",
    "trade_intent_normal_tp_mult",
    "trade_intent_normal_max_positions_mult",
    "trade_intent_normal_freq_mult",
    "trading_mode_preset",
    "economic_min_runner_notional_usdt",
    "economic_min_fee_mult",
    "exhaustion_tighten_score_threshold",
    "exhaustion_tighten_trail_mult",
    "early_invalidation_base_min_hold_sec",
    "early_invalidation_min_hold_floor_sec",
    "early_invalidation_max_hold_cap_sec",
    "early_invalidation_failure_score_threshold",
    "early_invalidation_patience_score_threshold",
    "early_invalidation_near_tp_floor_ratio",
    "early_invalidation_near_tp_grace_sec",
    "early_invalidation_chop_strictness_mult",
    "early_invalidation_trend_patience_mult",
    "winner_patience_mfe_roi_floor",
    "winner_patience_runner_quality_floor",
    "winner_patience_consensus_floor",
    "symbol_direction_loss_threshold",
    "symbol_direction_mild_loss_threshold",
    "symbol_direction_confirm_loss_threshold",
    "symbol_direction_cooldown_loss_threshold",
    "symbol_direction_cooldown_minutes",
    "symbol_direction_mild_conf_bump",
    "symbol_direction_loss_conf_bump",
    "symbol_direction_min_consensus",
    "symbol_direction_quality_override",
    "symbol_direction_loss_size_mult",
    "symbol_direction_mild_size_mult",
    "symbol_direction_memory_decay_hours",
    "net_expectancy_gate_enabled",
    "net_expectancy_degraded_penalty_enabled",
    "micro_meaningful_profit_enabled",
    "minimum_net_capture_fee_multiple",
}

# Per-engine slot-mode keys are hot-adjustable at runtime.
for _engine_name in ENGINE_LANES:
    DEFAULTS.setdefault(f"engine_{_engine_name.lower()}_slot_mode", "fixed")
    DEFAULTS.setdefault(
        f"engine_{_engine_name.lower()}_role",
        DEFAULT_ENGINE_ROLES.get(_engine_name, "hunter"),
    )
    _HOT_RUNTIME_VALUE_KEYS.add(f"engine_{_engine_name.lower()}_slot_mode")
    _HOT_RUNTIME_VALUE_KEYS.add(f"engine_{_engine_name.lower()}_role")
    _HOT_RUNTIME_VALUE_KEYS.add(f"engine_{_engine_name.lower()}_mode")
    _HOT_RUNTIME_VALUE_KEYS.add(f"engine_{_engine_name.lower()}_max_pos")

_HOT_RUNTIME_KEYS = set(_LIVE_TOGGLE_KEYS) | set(_HOT_RUNTIME_VALUE_KEYS)
for _group in _BOOL_ALIAS_GROUPS.values():
    _HOT_RUNTIME_KEYS.update(_group)


def _ensure_dir() -> None:
    _PATH.parent.mkdir(parents=True, exist_ok=True)


def _alias_group_for(key: str) -> tuple[str | None, tuple[str, ...]]:
    for canonical, group in _BOOL_ALIAS_GROUPS.items():
        if key in group:
            return canonical, group
    return None, ()


def _value_alias_group_for(key: str) -> tuple[str | None, tuple[str, ...]]:
    for canonical, group in _VALUE_ALIAS_GROUPS.items():
        if key in group:
            return canonical, group
    return None, ()


def _engine_from_enabled_key(key: str) -> str | None:
    if not (key.startswith("engine_") and key.endswith("_enabled")):
        return None
    engine_name = key[len("engine_"):-len("_enabled")].upper()
    return engine_name if engine_name in ENGINE_LANES else None


def _resolve_bool_group_value(canonical: str, source: dict | None = None) -> bool:
    src = source if isinstance(source, dict) else _current
    for name in _BOOL_ALIAS_GROUPS.get(canonical, (canonical,)):
        if name in src:
            return bool(src.get(name))
    return bool(DEFAULTS.get(canonical, False))


def _resolve_value_group_value(canonical: str, source: dict | None = None):
    src = source if isinstance(source, dict) else _current
    for name in _VALUE_ALIAS_GROUPS.get(canonical, (canonical,)):
        if name in src:
            return src.get(name)
    return deepcopy(DEFAULTS.get(canonical))


def _resolve_engine_mode(engine_name: str, source: dict | None = None) -> str:
    src = source if isinstance(source, dict) else _current
    key_mode = f"engine_{engine_name.lower()}_mode"
    key_enabled = f"engine_{engine_name.lower()}_enabled"
    mode = str(src.get(key_mode, "")).lower()
    if mode in VALID_ENGINE_MODES:
        return mode
    if key_enabled in src:
        return "live" if bool(src.get(key_enabled)) else "disabled"
    default_mode = str(DEFAULTS.get(key_mode, "disabled")).lower()
    return default_mode if default_mode in VALID_ENGINE_MODES else "disabled"


def _sync_runtime_aliases(source: dict | None = None) -> None:
    for canonical, group in _BOOL_ALIAS_GROUPS.items():
        resolved = _resolve_bool_group_value(canonical, source)
        for key in group:
            _current[key] = resolved
    for canonical, group in _VALUE_ALIAS_GROUPS.items():
        resolved = _resolve_value_group_value(canonical, source)
        for key in group:
            _current[key] = resolved
    for engine_name in ENGINE_LANES:
        key_mode = f"engine_{engine_name.lower()}_mode"
        key_enabled = f"engine_{engine_name.lower()}_enabled"
        mode = _resolve_engine_mode(engine_name, source)
        _current[key_mode] = mode
        _current[key_enabled] = mode in ("live", "probation")


def _read_settings_file() -> dict | None:
    try:
        if not _PATH.exists():
            return None
        return json.loads(_PATH.read_text(encoding="utf-8"))
    except Exception as e:
        logger.warning("Settings disk refresh failed: %s", e)
        return None


def _canonical_disk_snapshot() -> dict:
    snapshot = deepcopy(_current)
    for canonical, group in _BOOL_ALIAS_GROUPS.items():
        for key in group:
            if key != canonical:
                snapshot.pop(key, None)
    for canonical, group in _VALUE_ALIAS_GROUPS.items():
        for key in group:
            if key != canonical:
                snapshot.pop(key, None)
    for engine_name in ENGINE_LANES:
        snapshot.pop(f"engine_{engine_name.lower()}_enabled", None)
    return snapshot


def _hot_reload_runtime_toggles_if_changed() -> None:
    global _last_load_mtime, _last_hot_reload_check
    now = time.monotonic()
    if now - _last_hot_reload_check < 0.5:
        return
    _last_hot_reload_check = now
    try:
        if not _PATH.exists():
            return
        mtime = _PATH.stat().st_mtime
    except Exception:
        return
    if _last_load_mtime is None:
        _last_load_mtime = mtime
        return
    if mtime <= _last_load_mtime:
        return

    data = _read_settings_file()
    if not isinstance(data, dict):
        _last_load_mtime = mtime
        return

    old_values = {
        "lane_budget_enabled": _resolve_bool_group_value("lane_budget_enabled"),
        "engine_weight_adaptation_enabled": _resolve_bool_group_value("engine_weight_adaptation_enabled"),
        "confirm_path_enabled": _resolve_bool_group_value("confirm_path_enabled"),
        "dynamic_sizing_enabled": _resolve_bool_group_value("dynamic_sizing_enabled"),
    }
    for key in _LIVE_TOGGLE_KEYS:
        if key in old_values:
            continue
        old_values[key] = bool(_current.get(key, DEFAULTS.get(key, False)))
    for key in _HOT_RUNTIME_VALUE_KEYS:
        old_values[key] = deepcopy(_current.get(key, DEFAULTS.get(key)))

    _sync_runtime_aliases(source=data)
    for key in _LIVE_TOGGLE_KEYS:
        if key in _BOOL_ALIAS_GROUPS:
            continue
        if key in data:
            _current[key] = bool(data.get(key))
    for key in _HOT_RUNTIME_VALUE_KEYS:
        if key in data:
            _current[key] = deepcopy(data.get(key))
    _last_load_mtime = mtime

    new_values = {
        "lane_budget_enabled": _resolve_bool_group_value("lane_budget_enabled"),
        "engine_weight_adaptation_enabled": _resolve_bool_group_value("engine_weight_adaptation_enabled"),
        "confirm_path_enabled": _resolve_bool_group_value("confirm_path_enabled"),
        "dynamic_sizing_enabled": _resolve_bool_group_value("dynamic_sizing_enabled"),
    }
    for key in _LIVE_TOGGLE_KEYS:
        if key in new_values:
            continue
        new_values[key] = bool(_current.get(key, DEFAULTS.get(key, False)))
    for key in _HOT_RUNTIME_VALUE_KEYS:
        new_values[key] = deepcopy(_current.get(key, DEFAULTS.get(key)))

    for key, old_value in old_values.items():
        new_value = new_values.get(key, old_value)
        if old_value != new_value:
            logger.info("SETTINGS: %s %s -> %s (disk reload)", key, old_value, new_value)


def _normalize_dependent_bounds() -> None:
    """Repair persisted min/max inversions that can make runtime math unusable."""
    try:
        risk_min = float(_current.get("realized_risk_cap_min_usdt", DEFAULTS.get("realized_risk_cap_min_usdt", 1.0)) or 0.0)
        risk_max = float(_current.get("realized_risk_cap_max_usdt", DEFAULTS.get("realized_risk_cap_max_usdt", 3.0)) or 0.0)
        if risk_max < risk_min:
            _current["realized_risk_cap_min_usdt"] = risk_max
            _current["realized_risk_cap_max_usdt"] = risk_min
            logger.warning(
                "Normalized realized risk cap bounds: min %.2f max %.2f",
                _current["realized_risk_cap_min_usdt"],
                _current["realized_risk_cap_max_usdt"],
            )
    except Exception:
        pass
    try:
        target_min = float(_current.get("micro_target_min_pct_balance", DEFAULTS.get("micro_target_min_pct_balance", 300.0)) or 0.0)
        target_max = float(_current.get("micro_target_max_pct_balance", DEFAULTS.get("micro_target_max_pct_balance", 900.0)) or 0.0)
        if target_max < target_min:
            _current["micro_target_max_pct_balance"] = target_min
    except Exception:
        pass


def load() -> dict:
    global _current, _last_load_mtime
    try:
        if _PATH.exists():
            data   = json.loads(_PATH.read_text(encoding="utf-8"))
            merged = deepcopy(DEFAULTS)
            for key in DEFAULTS:
                if key == "tiers":
                    for tier in DEFAULTS["tiers"]:
                        if tier in data.get("tiers", {}):
                            for f in DEFAULTS["tiers"][tier]:
                                if f in data["tiers"][tier]:
                                    merged["tiers"][tier][f] = data["tiers"][tier][f]
                elif key in data:
                    merged[key] = data[key]
            _current = merged
            _sync_runtime_aliases(source=data)
            _normalize_dependent_bounds()
            _last_load_mtime = _PATH.stat().st_mtime
    except Exception as e:
        logger.warning("Settings load failed (%s) — using defaults.", e)
        _current = deepcopy(DEFAULTS)
        _sync_runtime_aliases()
        _normalize_dependent_bounds()
    return _current


def save() -> None:
    global _last_load_mtime
    try:
        if "PYTEST_CURRENT_TEST" in os.environ and _PATH == Path("data/settings.json"):
            return
        _ensure_dir()
        _sync_runtime_aliases()
        _PATH.write_text(json.dumps(_canonical_disk_snapshot(), indent=2, ensure_ascii=False), encoding="utf-8")
        _last_load_mtime = _PATH.stat().st_mtime if _PATH.exists() else _last_load_mtime
    except Exception as e:
        logger.warning("Settings save failed: %s", e)


def get() -> dict:
    return _current

def get_val(key: str, default=None):
    if key in _HOT_RUNTIME_KEYS:
        _hot_reload_runtime_toggles_if_changed()
    canonical, _group = _alias_group_for(key)
    if canonical:
        return _resolve_bool_group_value(canonical)
    value_canonical, _value_group = _value_alias_group_for(key)
    if value_canonical:
        return _resolve_value_group_value(value_canonical)
    legacy_engine = _engine_from_enabled_key(key)
    if legacy_engine:
        return get_engine_mode(legacy_engine) in ("live", "probation")
    return _current.get(key, default)


def get_tp_distance_scales(mode: str | None = None) -> tuple[float, float]:
    active_mode = str(mode or get_val("tp_distance_mode", "balanced") or "balanced").lower()
    if active_mode == "conservative":
        return 0.95, 1.00
    if active_mode == "extended":
        return 1.15, 1.30
    return 1.00, 1.00


def get_effective_min_order_usdt(balance_usdt: float | None = None) -> float:
    """Return the live execution floor for the current account size."""
    base_floor = float(get_val("min_order_usdt", 100.0) or 100.0)
    if not get_val("adaptive_min_order_floor_enabled", True):
        return max(base_floor, 1.0)
    if balance_usdt is None:
        return max(base_floor, 1.0)
    if get_val("micro_sizing_enabled", True):
        try:
            import micro_sizing as _micro_sizing

            return float(_micro_sizing.account_floor_usdt(balance_usdt, settings=__import__(__name__)))
        except Exception:
            pass

    micro_max = float(get_val("v6_micro_max_balance", 100.0) or 100.0)
    growth_max = float(get_val("v6_growth_max_balance", 1000.0) or 1000.0)
    v6_floor = float(get_val("v6_min_position_size_usdt", 25.0) or 25.0)

    if balance_usdt < micro_max:
        profile_floor = float(get_val("min_order_usdt_micro", v6_floor) or v6_floor)
    elif balance_usdt < growth_max:
        profile_floor = float(get_val("min_order_usdt_growth", max(v6_floor, 50.0)) or max(v6_floor, 50.0))
    else:
        profile_floor = float(get_val("min_order_usdt_pro", base_floor) or base_floor)

    return max(min(base_floor, profile_floor), 1.0)


def get_debug_override_snapshot() -> dict:
    """Return a normalized snapshot of the debug override / blocker probe settings."""

    def _norm_mode(value: str | None) -> str:
        raw = str(value or "off").strip().lower()
        return raw if raw in {"off", "probe", "live_override"} else "off"

    def _norm_level(value: str | None) -> str:
        raw = str(value or "normal").strip().lower()
        return raw if raw in {"normal", "relaxed", "unlimited"} else "normal"

    mode = _norm_mode(get_val("debug_override_mode", "off"))
    levels = {
        "global_cap": _norm_level(get_val("debug_override_global_cap", "normal")),
        "engine_cap": _norm_level(get_val("debug_override_engine_cap", "normal")),
        "weekly_cap": _norm_level(get_val("debug_override_weekly_cap", "normal")),
        "exposure_cap": _norm_level(get_val("debug_override_exposure_cap", "normal")),
        "correlation_cap": _norm_level(get_val("debug_override_correlation_cap", "normal")),
        "same_direction_cap": _norm_level(get_val("debug_override_same_direction_cap", "normal")),
        "portfolio_direction_cap": _norm_level(get_val("debug_override_portfolio_direction_cap", "normal")),
        "lane_budget": _norm_level(get_val("debug_override_lane_budget", "normal")),
    }
    active = {k: v for k, v in levels.items() if v != "normal"}
    return {
        "mode": mode,
        "levels": levels,
        "active": active,
        "active_count": len(active),
    }


_NUMERIC_SETTING_LIMITS: dict[str, tuple[float, float]] = {
    "position_size_usdt": (1.0, 1_000_000.0),
    "v6_base_position_size_usdt": (1.0, 1_000_000.0),
    "v6_min_position_size_usdt": (1.0, 1_000_000.0),
    "v6_max_position_size_usdt": (1.0, 5_000_000.0),
    "internal_sl_confirm_secs": (0.0, 300.0),
    "position_size_pct": (0.0, 10.0),
    "risk_pct": (0.0, 100.0),
    "position_size_cap_pct_balance": (0.0, 10_000.0),
    "position_size_cap_usdt": (1.0, 5_000_000.0),
    "v6_max_position_pct_of_balance": (0.0, 100.0),
    "realized_risk_cap_min_usdt": (0.0, 1_000_000.0),
    "realized_risk_cap_max_usdt": (0.0, 1_000_000.0),
    "micro_min_order_usdt_floor": (1.0, 1_000_000.0),
    "micro_margin_buffer_min_usdt": (0.0, 1_000_000.0),
    "micro_margin_buffer_pct_balance": (0.0, 100.0),
    "micro_margin_buffer_max_usdt": (0.0, 1_000_000.0),
    "micro_target_min_pct_balance": (0.0, 10_000.0),
    "micro_target_max_pct_balance": (0.0, 10_000.0),
    "leverage_default": (1.0, 125.0),
    "leverage_min": (1.0, 125.0),
    "leverage_max": (1.0, 125.0),
    "breakeven_secure_fee_mult": (0.0, 10.0),
    "breakeven_secure_floor_mult": (0.0, 10.0),
    "partial_skip_log_cooldown_sec": (0.0, 43_200.0),
    "economic_min_runner_notional_usdt": (0.0, 1_000_000.0),
    "economic_min_fee_mult": (0.0, 100.0),
    "positive_mfe_guard_mfe_roi_trigger": (0.0, 100.0),
    "positive_mfe_guard_min_profit_usdt": (0.0, 1_000_000.0),
    "positive_mfe_guard_fee_mult": (0.0, 100.0),
    "positive_mfe_guard_lock_roi_pct": (0.0, 100.0),
    "positive_mfe_guard_min_age_sec": (0.0, 86_400.0),
    "positive_mfe_guard_max_lock_levels": (0.0, 10.0),
    "fee_bleed_guard_min_net_profit_usdt": (0.0, 1_000_000.0),
    "fee_bleed_guard_max_fee_share": (0.0, 1.0),
    "fee_bleed_guard_min_partial_notional_usdt": (0.0, 1_000_000.0),
    "exhaustion_tighten_score_threshold": (0.0, 10.0),
    "exhaustion_tighten_trail_mult": (0.1, 1.5),
    "continuation_runner_min_roi": (0.0, 100.0),
    "continuation_runner_score_threshold": (0.0, 10.0),
    "continuation_runner_tp2_close_mult": (0.0, 1.0),
    "continuation_runner_trail_mult": (0.1, 10.0),
    "partial_tp1_close_pct": (0.0, 100.0),
    "partial_tp2_close_pct": (0.0, 100.0),
    "partial_tp1_max_close_pct": (0.0, 100.0),
    "partial_tp2_max_close_pct": (0.0, 100.0),
    "runner_core_min_remaining_pct": (0.0, 100.0),
    "continuation_runner_core_min_pct": (0.0, 100.0),
    "weekly_cap_degraded_conf_grace": (0.0, 3.0),
    "repeat_loss_guard_window_mins": (1.0, 10_080.0),
    "repeat_loss_guard_min_losses": (1.0, 10.0),
    "repeat_loss_guard_extra_conf": (0.0, 5.0),
    "symbol_instability_size_mult": (0.05, 1.0),
    "early_invalidation_min_age_sec": (0.0, 3600.0),
    "early_invalidation_max_age_min": (1.0, 240.0),
    "early_invalidation_roi_pct": (0.1, 50.0),
    "early_invalidation_confirm_secs": (0.0, 120.0),
    "early_invalidation_base_min_hold_sec": (0.0, 3600.0),
    "early_invalidation_min_hold_floor_sec": (0.0, 3600.0),
    "early_invalidation_max_hold_cap_sec": (1.0, 14_400.0),
    "early_invalidation_failure_score_threshold": (0.0, 20.0),
    "early_invalidation_patience_score_threshold": (0.0, 20.0),
    "early_invalidation_near_tp_floor_ratio": (0.0, 1.0),
    "early_invalidation_near_tp_grace_sec": (0.0, 3600.0),
    "early_invalidation_chop_strictness_mult": (0.0, 5.0),
    "early_invalidation_trend_patience_mult": (0.0, 5.0),
    "winner_patience_mfe_roi_floor": (0.0, 20.0),
    "winner_patience_runner_quality_floor": (0.0, 20.0),
    "winner_patience_consensus_floor": (0.0, 20.0),
    "symbol_direction_loss_threshold": (1.0, 20.0),
    "symbol_direction_mild_loss_threshold": (1.0, 20.0),
    "symbol_direction_confirm_loss_threshold": (1.0, 20.0),
    "symbol_direction_cooldown_loss_threshold": (1.0, 50.0),
    "symbol_direction_cooldown_minutes": (0.0, 1440.0),
    "symbol_direction_mild_conf_bump": (0.0, 5.0),
    "symbol_direction_loss_conf_bump": (0.0, 5.0),
    "symbol_direction_min_consensus": (0.0, 10.0),
    "symbol_direction_quality_override": (0.0, 500.0),
    "symbol_direction_loss_size_mult": (0.05, 1.0),
    "symbol_direction_mild_size_mult": (0.05, 1.0),
    "symbol_direction_memory_decay_hours": (0.0, 720.0),
    "net_expectancy_min_cost_mult": (0.0, 20.0),
    "net_expectancy_weak_cost_mult": (0.0, 30.0),
    "net_expectancy_soft_size_mult": (0.05, 1.0),
    "net_expectancy_strong_quality_floor": (0.0, 500.0),
    "net_expectancy_default_target_move_pct": (0.0, 100.0),
    "net_expectancy_default_slippage_pct": (0.0, 10.0),
    "micro_meaningful_profit_close_pct": (0.0, 100.0),
    "micro_meaningful_profit_max_close_pct": (0.0, 100.0),
    "micro_meaningful_profit_runner_floor_pct": (0.0, 100.0),
    "micro_meaningful_profit_quality_floor": (0.0, 20.0),
    "engine_candidate_limit_default": (0.0, 200.0),
    "engine_candidate_limit_hachiman": (0.0, 200.0),
    "engine_candidate_limit_hayate": (0.0, 200.0),
    "engine_candidate_limit_kagutsuchi": (0.0, 200.0),
    "engine_candidate_limit_ryujin": (0.0, 200.0),
    "role_consensus_conf_lift_min_count": (1.0, 10.0),
    "role_consensus_conf_lift_min_strength": (0.0, 20.0),
    "role_consensus_conf_lift_min_quality": (0.0, 150.0),
    "role_consensus_conf_lift_max": (0.0, 5.0),
}


def _sanitize_runtime_value(key: str, value):
    if key not in _NUMERIC_SETTING_LIMITS:
        if key == "minimum_net_capture_fee_multiple":
            try:
                numeric = float(value)
            except Exception:
                logger.warning("Rejected invalid setting %s=%r; preserving previous value", key, value)
                return _current.get(key, DEFAULTS.get(key)), False
            if numeric != numeric or numeric in (float("inf"), float("-inf")) or numeric < 0.0:
                logger.warning("Rejected non-finite or negative setting %s=%r; preserving previous value", key, value)
                return _current.get(key, DEFAULTS.get(key)), False
            return numeric, True
        return value, True
    try:
        numeric = float(value)
    except Exception:
        logger.warning("Rejected invalid setting %s=%r; preserving previous value", key, value)
        return _current.get(key, DEFAULTS.get(key)), False
    if numeric != numeric or numeric in (float("inf"), float("-inf")):
        logger.warning("Rejected non-finite setting %s=%r; preserving previous value", key, value)
        return _current.get(key, DEFAULTS.get(key)), False
    low, high = _NUMERIC_SETTING_LIMITS[key]
    if numeric < low or numeric > high:
        logger.warning("Rejected out-of-range setting %s=%r not in [%.4f, %.4f]", key, value, low, high)
        return _current.get(key, DEFAULTS.get(key)), False
    if key == "v6_min_position_size_usdt":
        current_max = float(_current.get("v6_max_position_size_usdt", DEFAULTS.get("v6_max_position_size_usdt", numeric)) or numeric)
        if numeric > current_max:
            logger.warning("Rejected min size %.2f above max size %.2f; preserving previous value", numeric, current_max)
            return _current.get(key, DEFAULTS.get(key)), False
    if key == "v6_max_position_size_usdt":
        current_min = float(_current.get("v6_min_position_size_usdt", DEFAULTS.get("v6_min_position_size_usdt", numeric)) or numeric)
        if numeric < current_min:
            logger.warning("Rejected max size %.2f below min size %.2f; preserving previous value", numeric, current_min)
            return _current.get(key, DEFAULTS.get(key)), False
    if key == "leverage_min":
        current_max = float(_current.get("leverage_max", DEFAULTS.get("leverage_max", numeric)) or numeric)
        if numeric > current_max:
            return _current.get(key, DEFAULTS.get(key)), False
    if key == "leverage_max":
        current_min = float(_current.get("leverage_min", DEFAULTS.get("leverage_min", numeric)) or numeric)
        if numeric < current_min:
            return _current.get(key, DEFAULTS.get(key)), False
    if key == "realized_risk_cap_min_usdt":
        current_max = float(_current.get("realized_risk_cap_max_usdt", DEFAULTS.get("realized_risk_cap_max_usdt", numeric)) or numeric)
        if numeric > current_max:
            logger.warning("Rejected risk cap min %.2f above max %.2f; preserving previous value", numeric, current_max)
            return _current.get(key, DEFAULTS.get(key)), False
    if key == "realized_risk_cap_max_usdt":
        current_min = float(_current.get("realized_risk_cap_min_usdt", DEFAULTS.get("realized_risk_cap_min_usdt", numeric)) or numeric)
        if numeric < current_min:
            logger.warning("Rejected risk cap max %.2f below min %.2f; preserving previous value", numeric, current_min)
            return _current.get(key, DEFAULTS.get(key)), False
    if key == "micro_target_max_pct_balance":
        current_min = float(_current.get("micro_target_min_pct_balance", DEFAULTS.get("micro_target_min_pct_balance", numeric)) or numeric)
        if numeric < current_min:
            logger.warning("Rejected micro target max %.2f below min %.2f; preserving previous value", numeric, current_min)
            return _current.get(key, DEFAULTS.get(key)), False
    if key.startswith("leverage"):
        return int(numeric), True
    return numeric, True


def set_val(key: str, value, actor: str = "operator", reason: str = "") -> None:
    canonical, group = _alias_group_for(key)
    journal_key = canonical or key
    if canonical:
        old_value = _resolve_bool_group_value(canonical)
        new_value = bool(value)
        for alias_key in group:
            _current[alias_key] = new_value
    else:
        value_canonical, value_group = _value_alias_group_for(key)
        if value_canonical:
            journal_key = value_canonical
            old_value = _resolve_value_group_value(value_canonical)
            new_value, ok = _sanitize_runtime_value(value_canonical, value)
            if not ok:
                return
            for alias_key in value_group:
                _current[alias_key] = new_value
        else:
            legacy_engine = _engine_from_enabled_key(key)
            if legacy_engine:
                old_value = bool(get_val(key, False))
                new_mode = "live" if bool(value) else "disabled"
                set_engine_mode(legacy_engine, new_mode, actor=actor, reason=reason or "legacy enabled alias")
                return
            old_value = _current.get(key)
            new_value, ok = _sanitize_runtime_value(key, value)
            if not ok:
                return
            _current[key] = new_value
    save()
    if journal_key in _LIVE_TOGGLE_KEYS:
        logger.info("SETTINGS: %s %s -> %s", journal_key, old_value, new_value)
    else:
        logger.info("Settings: %s = %s (was %s)", key, new_value, old_value)
    _journal_write(journal_key, old_value, new_value, actor, reason)


def bump_counter(key: str, delta: int = 1) -> int:
    current = int(_current.get(key, DEFAULTS.get(key, 0)) or 0)
    new_value = current + int(delta)
    _current[key] = new_value
    save()
    return new_value

def get_tier(name: str) -> dict:
    return _current["tiers"].get(name.upper(), deepcopy(DEFAULTS["tiers"].get(name.upper(), {})))

def set_tier_field(tier: str, field: str, value: float) -> None:
    tier = tier.upper()
    if tier in _current["tiers"] and field in _current["tiers"][tier]:
        _current["tiers"][tier][field] = value
        save()

def set_daily_cap_enabled(enabled: bool) -> None:
    set_val("daily_cap_enabled", bool(enabled))

def is_daily_cap_enabled() -> bool:
    return bool(_current.get("daily_cap_enabled", False))

def toggle_engine(name: str) -> bool:
    """Toggle a category engine on/off. Returns new state (True=enabled)."""
    key     = f"engine_{name.lower()}_enabled"
    current = bool(_current.get(key, True))
    set_val(key, not current)
    return not current

def reset_to_defaults() -> None:
    global _current
    _current = deepcopy(DEFAULTS)
    _sync_runtime_aliases()
    save()
    logger.info("Settings reset to defaults.")


# ── Append-only operator change journal ──────────────────────────────────────

def _journal_write(key: str, old_value, new_value, actor: str = "operator", reason: str = "") -> None:
    """Append one line to data/change_journal.jsonl."""
    try:
        _JOURNAL_PATH.parent.mkdir(parents=True, exist_ok=True)
        entry = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "actor": actor,
            "key": key,
            "old": old_value,
            "new": new_value,
            "reason": reason,
        }
        with _JOURNAL_PATH.open("a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")
    except Exception as e:
        logger.debug("Journal write failed: %s", e)


def get_recent_journal(n: int = 20) -> list[dict]:
    """Return the last N journal entries (newest first)."""
    try:
        if not _JOURNAL_PATH.exists():
            return []
        lines = _JOURNAL_PATH.read_text(encoding="utf-8").strip().split("\n")
        entries = []
        for line in lines[-n:]:
            if line.strip():
                entries.append(json.loads(line))
        entries.reverse()
        return entries
    except Exception:
        return []


# ── Config drift detection ───────────────────────────────────────────────────

def detect_drift() -> list[dict]:
    """Compare persisted settings.json against code DEFAULTS.
    Returns list of {key, default, persisted} for any mismatches.
    Only checks keys that exist in DEFAULTS (ignores extra persisted keys).
    """
    drifts = []
    try:
        if not _PATH.exists():
            return []
        data = json.loads(_PATH.read_text(encoding="utf-8"))
        for key, default_val in DEFAULTS.items():
            if key == "tiers":
                continue  # skip nested tiers
            persisted_val = data.get(key)
            if persisted_val is None:
                continue  # key not in persisted file = will use default, no drift
            if persisted_val != default_val:
                drifts.append({
                    "key": key,
                    "default": default_val,
                    "persisted": persisted_val,
                })
    except Exception as e:
        logger.warning("Drift detection failed: %s", e)
    return drifts


def log_drift_on_startup() -> None:
    """Called once at startup to log config drift between defaults and persisted."""
    drifts = detect_drift()
    if not drifts:
        logger.info("Config drift check: no drift detected (defaults == persisted).")
        return
    logger.info("Config drift check: %d key(s) differ from defaults:", len(drifts))
    for d in drifts:
        logger.info("  DRIFT: %s — default=%s, persisted=%s", d["key"], d["default"], d["persisted"])


# ── Engine mode helpers ──────────────────────────────────────────────────────

def get_engine_mode(engine_name: str) -> str:
    """Return the operating mode for an engine: disabled/shadow/probation/live."""
    _hot_reload_runtime_toggles_if_changed()
    return _resolve_engine_mode(engine_name)


def set_engine_mode(engine_name: str, mode: str, actor: str = "operator", reason: str = "") -> None:
    """Set engine mode and sync the legacy enabled flag."""
    mode = mode.lower()
    if mode not in VALID_ENGINE_MODES:
        logger.warning("Invalid engine mode '%s' for %s", mode, engine_name)
        return
    key_mode = f"engine_{engine_name.lower()}_mode"
    key_enabled = f"engine_{engine_name.lower()}_enabled"
    old_mode = get_engine_mode(engine_name)
    _current[key_mode] = mode
    # Sync legacy enabled flag: live/probation = enabled, shadow/disabled = disabled
    _current[key_enabled] = mode in ("live", "probation")
    save()
    _journal_write(key_mode, old_mode, mode, actor, reason)
    logger.info("Engine mode: %s %s → %s (%s: %s)", engine_name, old_mode, mode, actor, reason)


def is_engine_enabled(engine_name: str) -> bool:
    """True if the engine should scan in any non-disabled mode."""
    return get_engine_mode(engine_name) != "disabled"


def is_engine_live_or_probation(engine_name: str) -> bool:
    """True if engine is in a mode that places real orders."""
    return get_engine_mode(engine_name) in ("live", "probation")


def is_engine_shadow(engine_name: str) -> bool:
    """True if engine should scan but only record hypotheticals."""
    return get_engine_mode(engine_name) == "shadow"


def get_engine_lane(engine_name: str) -> str:
    """Return the lane this engine belongs to."""
    return ENGINE_LANES.get(engine_name.upper(), "trend")


def get_engine_role(engine_name: str) -> str:
    """Return the configurable contribution role for an engine."""
    engine_name = str(engine_name or "").upper()
    role = str(
        get_val(
            f"engine_{engine_name.lower()}_role",
            DEFAULT_ENGINE_ROLES.get(engine_name, "hunter"),
        )
        or "hunter"
    ).strip().lower()
    return role if role in VALID_ENGINE_ROLES else DEFAULT_ENGINE_ROLES.get(engine_name, "hunter")


def get_engine_role_weight(engine_name: str | None = None, role: str | None = None) -> float:
    """Return the soft contribution weight for an engine role."""
    resolved = str(role or get_engine_role(engine_name or "") or "hunter").strip().lower()
    return float(ENGINE_ROLE_WEIGHTS.get(resolved, 1.0))


def set_engine_role(engine_name: str, role: str, actor: str = "operator", reason: str = "") -> None:
    """Set an engine contribution role without changing its operating mode."""
    engine_name = str(engine_name or "").upper()
    role = str(role or "").strip().lower()
    if engine_name not in ENGINE_LANES:
        logger.warning("Invalid engine for role assignment: %s", engine_name)
        return
    if role not in VALID_ENGINE_ROLES:
        logger.warning("Invalid engine role '%s' for %s", role, engine_name)
        return
    key = f"engine_{engine_name.lower()}_role"
    old_role = get_engine_role(engine_name)
    _current[key] = role
    save()
    _journal_write(key, old_role, role, actor, reason)
    logger.info("Engine role: %s %s -> %s (%s: %s)", engine_name, old_role, role, actor, reason)


def get_lane_allowed_regimes(engine_name: str) -> list[str]:
    """Return allowed regimes for this engine's lane."""
    lane = get_engine_lane(engine_name)
    return LANE_REGIME_ALLOWLIST.get(lane, ["TRENDING", "RANGING", "VOLATILE"])


def is_automation_mutation_allowed() -> bool:
    """True if automation is allowed to mutate config/code."""
    return bool(_current.get("automation_mutation_enabled", False))


load()
log_drift_on_startup()

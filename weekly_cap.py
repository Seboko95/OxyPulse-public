"""
OxyPulse — weekly_cap.py

Adaptive weekly-cap governance for NEW trade decisions only.

Goal:
- Preserve a weekly risk guard without freezing the bot.
- High-quality setups have a path via reduced sizing + cooldown.
- No modification of existing open positions.
"""

from __future__ import annotations

from dataclasses import dataclass
import time

import settings_store as _s
import performance_tracker as _perf


_last_reduced_accept_ts: float = 0.0  # monotonic timestamp of last ACCEPT_REDUCED


@dataclass(frozen=True)
class WeeklyCapResult:
    decision: str  # ACCEPT | ACCEPT_REDUCED | BLOCK
    mode: str
    drawdown_pct: float
    threshold_pct: float
    recovery_threshold_pct: float
    min_confidence: float
    allow_high_conf_only: bool
    size_multiplier: float
    cooldown_seconds: float
    cooldown_active: bool
    high_quality: bool
    reason: str
    required_confidence: float = 0.0
    confidence_deficit: float = 0.0
    degraded_data: bool = False


def _norm_mode(value: str | None) -> str:
    raw = str(value or "").strip().lower()
    if raw in {"off", "soft", "strict", "adaptive"}:
        return raw
    return "adaptive"


def _norm_level(value: str | None) -> str:
    raw = str(value or "normal").strip().lower()
    return raw if raw in {"normal", "relaxed", "unlimited"} else "normal"


def get_weekly_drawdown_pct(balance: float) -> float:
    """Weekly drawdown pct defined as abs(net_weekly_pnl) / balance * 100 (losses only)."""
    bal = float(balance or 0.0)
    if bal <= 0:
        return 0.0
    weekly_net_loss = max(-float(getattr(_perf._state, "weekly_pnl_usdt", 0.0) or 0.0), 0.0)
    return round((weekly_net_loss / bal) * 100.0, 4)


def _effective_mode_and_thresholds() -> tuple[str, float, float, float, bool, float, float]:
    """Return (mode, thr, rec_thr, min_conf, allow_hq_only, size_red, cooldown)."""
    # Backward compat: weekly_cap_enabled False forces OFF.
    enabled = bool(_s.get_val("weekly_cap_enabled", True))
    mode = _norm_mode(_s.get_val("weekly_cap_mode", "adaptive"))
    if not enabled:
        mode = "off"

    # Backward compat: default thresholds derive from weekly_loss_pct if new keys aren't set.
    base_thr = float(_s.get_val("weekly_loss_pct", 8.0) or 8.0)
    thr = float(_s.get_val("weekly_cap_threshold_pct", base_thr) or base_thr)
    rec_default = max(thr * 0.6, 0.0)
    rec_thr = float(_s.get_val("weekly_cap_recovery_threshold_pct", rec_default) or rec_default)

    min_conf = float(_s.get_val("weekly_cap_min_confidence", 4.2) or 4.2)
    allow_hq_only = bool(_s.get_val("weekly_cap_allow_high_conf_only", True))
    size_red = float(_s.get_val("weekly_cap_size_reduction", 0.5) or 0.5)
    cooldown = float(_s.get_val("weekly_cap_cooldown_seconds", 120.0) or 120.0)

    # Sanity clamps.
    thr = max(thr, 0.0)
    rec_thr = max(min(rec_thr, thr), 0.0) if thr > 0 else max(rec_thr, 0.0)
    size_red = max(min(size_red, 1.0), 0.05)
    cooldown = max(cooldown, 0.0)

    return mode, thr, rec_thr, min_conf, allow_hq_only, size_red, cooldown


def mark_reduced_accept(now_monotonic: float | None = None) -> None:
    """Mark that a reduced-size trade was actually accepted/executed.

    This drives the cooldown window. Call this only after an entry is truly opened.
    """
    global _last_reduced_accept_ts
    _last_reduced_accept_ts = float(now_monotonic if now_monotonic is not None else time.monotonic())


def reset_state() -> None:
    """Reset weekly-cap internal cooldown memory (NEW decisions only)."""
    global _last_reduced_accept_ts
    _last_reduced_accept_ts = 0.0


def evaluate(
    *,
    balance: float,
    confidence: float,
    candidate_quality: float = 0.0,
    selector_bonus: float = 0.0,
    consensus_count: int = 0,
    engine_name: str = "",
    top_ranked: bool = False,
    override_level: str = "normal",
    mutate_state: bool = True,
    now_monotonic: float | None = None,
    degraded_data: bool = False,
    degraded_reason: str = "",
) -> WeeklyCapResult:
    """Evaluate weekly cap posture for a NEW candidate trade.

    Returns:
      WeeklyCapResult with decision ACCEPT / ACCEPT_REDUCED / BLOCK.
    """
    global _last_reduced_accept_ts

    mode, thr, rec_thr, min_conf, allow_hq_only, size_red, cooldown = _effective_mode_and_thresholds()
    dd = float(get_weekly_drawdown_pct(balance) or 0.0)
    lvl = _norm_level(override_level)

    # Debug override: unlimited disables weekly cap for NEW candidates only.
    if lvl == "unlimited":
        return WeeklyCapResult(
            decision="ACCEPT",
            mode=mode,
            drawdown_pct=dd,
            threshold_pct=thr,
            recovery_threshold_pct=rec_thr,
            min_confidence=min_conf,
            allow_high_conf_only=allow_hq_only,
            size_multiplier=1.0,
            cooldown_seconds=cooldown,
            cooldown_active=False,
            high_quality=False,
            reason="weekly_cap:override_unlimited",
        )

    # Debug override: relaxed widens thresholds without disabling the guard.
    if lvl == "relaxed":
        thr = float(thr) + max(1.5, float(thr) * 0.25)
        rec_thr = max(float(rec_thr), float(thr) * 0.60)
        # Slightly lower min_conf so near-HQ trades can still be reduced instead of blocked.
        min_conf = max(float(min_conf) - 0.25, 0.0)
        allow_hq_only = False

    # OFF: no impact.
    if mode == "off" or thr <= 0:
        return WeeklyCapResult(
            decision="ACCEPT",
            mode=mode,
            drawdown_pct=dd,
            threshold_pct=thr,
            recovery_threshold_pct=rec_thr,
            min_confidence=min_conf,
            allow_high_conf_only=allow_hq_only,
            size_multiplier=1.0,
            cooldown_seconds=cooldown,
            cooldown_active=False,
            high_quality=False,
            reason="weekly_cap:off",
        )

    conf = float(confidence or 0.0)
    is_selector = (str(engine_name or "").upper() == "HACHIMAN") or (float(selector_bonus or 0.0) > 0.0)
    multi_engine = int(consensus_count or 0) >= 2
    # "Top ranked" is a hint; if not provided, treat very high candidate_quality as top-tier.
    is_top = bool(top_ranked) or float(candidate_quality or 0.0) >= 88.0
    quality_context = bool(is_top or is_selector or multi_engine)
    high_quality = bool(conf >= min_conf and quality_context)
    degraded_grace = (
        float(_s.get_val("weekly_cap_degraded_conf_grace", 0.50) or 0.50)
        if bool(degraded_data)
        else 0.0
    )

    def _now() -> float:
        return float(now_monotonic if now_monotonic is not None else time.monotonic())

    def _cooldown_active() -> bool:
        if cooldown <= 0:
            return False
        now = _now()
        return bool(_last_reduced_accept_ts and (now - float(_last_reduced_accept_ts)) < float(cooldown))

    def _result(
        *,
        decision: str,
        mult: float,
        why: str,
        cd: bool = False,
        required: float = 0.0,
    ) -> WeeklyCapResult:
        return WeeklyCapResult(
            decision=decision,
            mode=mode,
            drawdown_pct=dd,
            threshold_pct=thr,
            recovery_threshold_pct=rec_thr,
            min_confidence=min_conf,
            allow_high_conf_only=allow_hq_only,
            size_multiplier=max(min(float(mult or 0.0), 1.0), 0.05) if decision != "BLOCK" else 0.0,
            cooldown_seconds=cooldown,
            cooldown_active=cd,
            high_quality=high_quality,
            reason=why,
            required_confidence=float(required or 0.0),
            confidence_deficit=round(max(float(required or 0.0) - conf, 0.0), 4) if required else 0.0,
            degraded_data=bool(degraded_data),
        )

    def _accept_reduced(mult: float, why: str, required: float = 0.0) -> WeeklyCapResult:
        now = _now()
        cd = _cooldown_active()
        if cd and not high_quality:
            return _result(decision="BLOCK", mult=0.0, why=f"weekly_cap:cooldown ({why})", cd=True, required=required)
        # Allow high-quality trades through even within cooldown; still reduced.
        if mutate_state:
            _last_reduced_accept_ts = now
        return _result(decision="ACCEPT_REDUCED", mult=mult, why=why, cd=cd, required=required)

    def _block(why: str, required: float = 0.0) -> WeeklyCapResult:
        return _result(decision="BLOCK", mult=0.0, why=why, cd=_cooldown_active(), required=required)

    # ADAPTIVE: two bands.
    #  1) recovery_threshold <= dd < threshold: soft posture (reduce size, block low confidence)
    #  2) dd >= threshold: allow only high-quality (reduced size), otherwise block
    if mode == "adaptive":
        if dd < rec_thr:
            return WeeklyCapResult(
                decision="ACCEPT",
                mode=mode,
                drawdown_pct=dd,
                threshold_pct=thr,
                recovery_threshold_pct=rec_thr,
                min_confidence=min_conf,
                allow_high_conf_only=allow_hq_only,
                size_multiplier=1.0,
                cooldown_seconds=cooldown,
                cooldown_active=False,
                high_quality=high_quality,
                reason="weekly_cap:normal",
            )
        if dd < thr:
            if conf < min_conf:
                if degraded_grace > 0 and quality_context and conf >= max(min_conf - degraded_grace, 0.0):
                    return _accept_reduced(
                        size_red,
                        f"weekly_cap:adaptive soft degraded reduced x{size_red:.2f} ({degraded_reason or 'degraded_data'})",
                        required=min_conf,
                    )
                return _block(f"weekly_cap:adaptive soft block conf<{min_conf:.2f}", required=min_conf)
            return _accept_reduced(size_red, f"weekly_cap:adaptive soft reduced x{size_red:.2f}", required=min_conf)
        # At/above threshold: high-quality only (reduced size + cooldown)
        if high_quality:
            mult = min(size_red * 0.6, 0.45)
            return _accept_reduced(mult, f"weekly_cap:adaptive HQ reduced x{mult:.2f}")
        return _block("weekly_cap:adaptive block (not_high_quality)")

    # SOFT behavior (only triggers at/above threshold).
    if mode == "soft":
        if dd < rec_thr:
            return WeeklyCapResult(
                decision="ACCEPT",
                mode=mode,
                drawdown_pct=dd,
                threshold_pct=thr,
                recovery_threshold_pct=rec_thr,
                min_confidence=min_conf,
                allow_high_conf_only=allow_hq_only,
                size_multiplier=1.0,
                cooldown_seconds=cooldown,
                cooldown_active=False,
                high_quality=high_quality,
                reason="weekly_cap:normal",
            )
        # Recovery band: reduce for qualified trades, block low confidence.
        if dd < thr:
            if conf < min_conf:
                if degraded_grace > 0 and quality_context and conf >= max(min_conf - degraded_grace, 0.0):
                    return _accept_reduced(
                        size_red,
                        f"weekly_cap:soft(recovery) degraded reduced x{size_red:.2f} ({degraded_reason or 'degraded_data'})",
                        required=min_conf,
                    )
                return _block(f"weekly_cap:soft(recovery) block conf<{min_conf:.2f}", required=min_conf)
            return _accept_reduced(size_red, f"weekly_cap:soft(recovery) reduced x{size_red:.2f}", required=min_conf)
        if conf < min_conf:
            return _block(f"weekly_cap:soft block conf<{min_conf:.2f}", required=min_conf)
        if allow_hq_only and not high_quality:
            return _block("weekly_cap:soft block (not_high_quality)")
        return _accept_reduced(size_red, f"weekly_cap:soft reduced x{size_red:.2f}")

    # STRICT behavior (only triggers at/above threshold).
    if mode == "strict":
        if dd < rec_thr:
            return WeeklyCapResult(
                decision="ACCEPT",
                mode=mode,
                drawdown_pct=dd,
                threshold_pct=thr,
                recovery_threshold_pct=rec_thr,
                min_confidence=min_conf,
                allow_high_conf_only=allow_hq_only,
                size_multiplier=1.0,
                cooldown_seconds=cooldown,
                cooldown_active=False,
                high_quality=high_quality,
                reason="weekly_cap:normal",
            )
        if dd < thr:
            # In recovery band, tighten progressively instead of treating early
            # recovery the same as near-threshold drawdown.
            band = max(float(thr) - float(rec_thr), 0.0001)
            recovery_progress = max(min((float(dd) - float(rec_thr)) / band, 1.0), 0.0)
            strict_add = 0.25 if recovery_progress >= 0.50 else 0.0
            strict_soft_conf = min_conf + strict_add
            if conf < strict_soft_conf:
                if degraded_grace > 0 and quality_context and conf >= max(strict_soft_conf - degraded_grace, 0.0):
                    mult = min(size_red * (0.9 if recovery_progress < 0.50 else 0.8), 0.80)
                    return _accept_reduced(
                        mult,
                        f"weekly_cap:strict(recovery stage={recovery_progress:.2f}) degraded reduced x{mult:.2f} ({degraded_reason or 'degraded_data'})",
                        required=strict_soft_conf,
                    )
                return _block(f"weekly_cap:strict(recovery stage={recovery_progress:.2f}) block conf<{strict_soft_conf:.2f}", required=strict_soft_conf)
            mult = min(size_red * (0.9 if recovery_progress < 0.50 else 0.8), 0.80)
            return _accept_reduced(mult, f"weekly_cap:strict(recovery stage={recovery_progress:.2f}) reduced x{mult:.2f}", required=strict_soft_conf)

        strict_conf = min_conf + 0.5
        if high_quality:
            # Allow reduced even above strict threshold when HQ.
            mult = min(size_red * 0.6, 0.45)
            return _accept_reduced(mult, f"weekly_cap:strict HQ reduced x{mult:.2f}")
        if allow_hq_only:
            return _block("weekly_cap:strict block (not_high_quality)")
        if conf < strict_conf:
            return _block(f"weekly_cap:strict block conf<{strict_conf:.2f}", required=strict_conf)
        mult = min(size_red * 0.6, 0.45)
        return _accept_reduced(mult, f"weekly_cap:strict reduced x{mult:.2f}")

    # Fallback.
    return WeeklyCapResult(
        decision="ACCEPT",
        mode=mode,
        drawdown_pct=dd,
        threshold_pct=thr,
        recovery_threshold_pct=rec_thr,
        min_confidence=min_conf,
        allow_high_conf_only=allow_hq_only,
        size_multiplier=1.0,
        cooldown_seconds=cooldown,
        cooldown_active=False,
        high_quality=high_quality,
        reason="weekly_cap:fallback",
    )

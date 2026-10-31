"""
OxyPulse - reset_manager.py
Safe session reset for tracking/diagnostics ONLY.

Critical constraints:
  - Must not touch open positions, OKX orders, or execution pipeline state.
  - Applies to NEW decisions only (clears internal tracking/counters/buffers).
"""
from __future__ import annotations

import logging
import time
from datetime import datetime, timezone

logger = logging.getLogger("OxyPulse.Reset")

_PROCESS_START_TS: float = time.time()


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def get_last_reset_timestamp() -> str:
    """Return the last session reset timestamp (ISO8601 UTC), or '' if never reset."""
    try:
        import performance_tracker as _pt

        ts = str(getattr(_pt._state, "session_reset_ts", "") or "")
        return ts
    except Exception:
        return ""


def get_session_uptime_seconds() -> float:
    """Uptime since last session reset, else since process start."""
    now = time.time()
    last = get_last_reset_timestamp()
    if last:
        try:
            dt = datetime.fromisoformat(last)
            return max(0.0, now - dt.timestamp())
        except Exception:
            pass
    return max(0.0, now - float(_PROCESS_START_TS))


def format_uptime(seconds: float) -> str:
    s = int(max(float(seconds or 0.0), 0.0))
    h = s // 3600
    m = (s % 3600) // 60
    sec = s % 60
    return f"{h:02d}:{m:02d}:{sec:02d}"


def reset_all() -> dict:
    """Reset internal tracking systems for a fresh performance session.

    Safe by design:
      - does not touch open positions
      - does not cancel/modify OKX orders
      - does not alter strategy settings

    Returns a summary dict useful for logging/diagnostics.
    """
    summary: dict[str, list[str] | dict] = {"ok": [], "failed": []}

    # 1) Performance tracking (persistent; sets session_reset_ts)
    try:
        import performance_tracker as _perf

        _perf.reset_session()
        summary["ok"].append("performance_tracker.reset_session")  # type: ignore[index]
    except Exception as e:
        summary["failed"].append(f"performance_tracker.reset_session:{type(e).__name__}")  # type: ignore[index]

    # 2) Weekly-cap cooldown memory
    try:
        import weekly_cap as _wk

        _wk.reset_state()
        summary["ok"].append("weekly_cap.reset_state")  # type: ignore[index]
    except Exception as e:
        summary["failed"].append(f"weekly_cap.reset_state:{type(e).__name__}")  # type: ignore[index]

    # 3) Winrate optimization learning state (engine weight adaptation persistence)
    try:
        import engine_weights as _ew

        _ew.reset_all_weights()
        summary["ok"].append("engine_weights.reset_all_weights")  # type: ignore[index]
    except Exception as e:
        summary["failed"].append(f"engine_weights.reset_all_weights:{type(e).__name__}")  # type: ignore[index]

    # 4) Per-engine stats / counters
    try:
        import category_tracker as _ct

        _ct.reset_session()
        summary["ok"].append("category_tracker.reset_session")  # type: ignore[index]
    except Exception as e:
        summary["failed"].append(f"category_tracker.reset_session:{type(e).__name__}")  # type: ignore[index]

    # 5) Diagnostics buffers (blocked candidates / probe annotations)
    try:
        import rejection_log as _rj

        _rj.clear()
        summary["ok"].append("rejection_log.clear")  # type: ignore[index]
    except Exception as e:
        summary["failed"].append(f"rejection_log.clear:{type(e).__name__}")  # type: ignore[index]

    # 6) Trade-frequency buckets (admission only; does not touch open positions)
    try:
        import capital_manager as _cm

        if hasattr(_cm, "reset_frequency_state"):
            _cm.reset_frequency_state()
        summary["ok"].append("capital_manager.reset_frequency_state")  # type: ignore[index]
    except Exception as e:
        summary["failed"].append(f"capital_manager.reset_frequency_state:{type(e).__name__}")  # type: ignore[index]

    # 7) Decision trace buffer (diagnostics only)
    try:
        import decision_trace as _dt

        _dt.clear()
        summary["ok"].append("decision_trace.clear")  # type: ignore[index]
    except Exception as e:
        summary["failed"].append(f"decision_trace.clear:{type(e).__name__}")  # type: ignore[index]

    logger.warning(
        "SESSION RESET EXECUTED | ts=%s | ok=%d failed=%d",
        _utc_now_iso(),
        len(summary["ok"]),
        len(summary["failed"]),
    )
    return dict(summary)

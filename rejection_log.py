"""
OxyPulse — rejection_log.py
In-memory ring buffer of recent candidate trade rejections.

One record per candidate that cleared signal scoring but was blocked by
any downstream filter. Thread-safe (GIL + deque).

Tags (canonical set):
    frequency       trade-frequency cap (hourly/daily)
    drawdown        drawdown guard paused new trades
    low_confidence  V6.1 weighted-confidence gate
    engine_cap      per-engine position ceiling
    correlation     correlated-group cap
    global_cap      global max-open-positions ceiling
    portfolio       portfolio gate (same direction/coin)
    blackswan       BTC blackswan halt
    weekly_cap      weekly loss cap
    daily_cap       daily loss cap
    open_risk       3-5-7 open-risk cap
    entry_filter    other entry_filter block (HTF, funding, session, OI)
    exposure        V6.1 exposure cap
    market_filter   V6.1 market quality (spread/vol/ATR)
    ai_gate         AI gatekeeper veto
    reserve         position-registry reserve conflict
    sizing          effective size below minimum
    cooldown        single-SL coin cooldown
    headroom        projected trade cannot fit same-direction executable headroom
    lane_budget     per-lane autonomy budget cap
    soft_accept     trade passed via soft-accept (one soft filter bypassed at high confidence)
    replaced_by_better_trade  slot replacement occurred (informational event; not a rejection)
    selection_policy  blocked by selection policy (e.g., best_score reserved slots)
"""
from __future__ import annotations

import threading
import time
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone

import settings_store as _s

_MAXLEN = 300   # keep last 300 rejections
_SUPPRESSIBLE_TAGS = {
    "frequency",
    "drawdown",
    "engine_cap",
    "correlation",
    "global_cap",
    "portfolio",
    "blackswan",
    "weekly_cap",
    "daily_cap",
    "open_risk",
    "entry_filter",
    "exposure",
    "market_filter",
    "ai_gate",
    "sizing",
    "cooldown",
    "headroom",
    "lane_budget",
}


@dataclass(frozen=True)
class Rejection:
    ts: str
    engine: str
    coin: str
    direction: str
    conf: float
    tag: str
    detail: str


_lock = threading.Lock()
_buf: deque[Rejection] = deque(maxlen=_MAXLEN)
_event_state: dict[tuple[str, str, str, str], dict] = {}
_candidate_state: dict[tuple[str, str, str], dict] = {}


def _event_key(engine: str, coin: str, direction: str, tag: str) -> tuple[str, str, str, str]:
    return (engine.upper(), coin.upper(), direction.upper(), tag)


def _candidate_key(engine: str, coin: str, direction: str) -> tuple[str, str, str]:
    return (engine.upper(), coin.upper(), direction.upper())


def record(engine: str, coin: str, direction: str,
           conf: float, tag: str, detail: str) -> dict:
    """Record a blocked candidate and return dedupe metadata."""
    ts = datetime.now(timezone.utc).strftime("%H:%M")
    now = time.monotonic()
    group_window = float(_s.get_val("blocked_candidate_group_window_secs", 300.0) or 300.0)
    cooldown_secs = float(_s.get_val("blocked_candidate_cooldown_secs", 180.0) or 180.0)
    suppress_repeats = bool(_s.get_val("blocked_candidate_suppress_repeats", True))
    event_key = _event_key(engine, coin, direction, tag)
    candidate_key = _candidate_key(engine, coin, direction)

    with _lock:
        _buf.append(Rejection(
            ts=ts,
            engine=engine,
            coin=coin,
            direction=direction,
            conf=round(float(conf), 2),
            tag=tag,
            detail=detail,
        ))

        event = _event_state.get(event_key)
        if event and now - float(event.get("last_seen", 0.0)) <= group_window:
            repeat_count = int(event.get("repeat_count", 0)) + 1
        else:
            repeat_count = 1
        _event_state[event_key] = {
            "last_seen": now,
            "repeat_count": repeat_count,
        }

        if tag in _SUPPRESSIBLE_TAGS:
            _candidate_state[candidate_key] = {
                "until": now + cooldown_secs,
                "tag": tag,
                "detail": detail,
                "repeat_count": repeat_count,
            }

    return {
        "repeat_count": repeat_count,
        "suppressed_log": bool(suppress_repeats and repeat_count > 1),
    }


def should_skip_candidate(engine: str, coin: str, direction: str) -> dict | None:
    """Return cooldown state when a repeated blocked candidate should be skipped."""
    if not bool(_s.get_val("blocked_candidate_suppress_repeats", True)):
        return None

    now = time.monotonic()
    candidate_key = _candidate_key(engine, coin, direction)
    with _lock:
        state = _candidate_state.get(candidate_key)
        if not state:
            return None
        if state.get("tag") not in _SUPPRESSIBLE_TAGS:
            return None
        if now >= float(state.get("until", 0.0)):
            _candidate_state.pop(candidate_key, None)
            return None
        return dict(state)


def get_recent(n: int = 25) -> list[Rejection]:
    """Return the last n rejections, newest last."""
    with _lock:
        return list(_buf)[-n:]


def get_summary() -> dict[str, int]:
    """Count of rejections by tag across the full buffer."""
    with _lock:
        items = list(_buf)
    counts: dict[str, int] = {}
    for r in items:
        counts[r.tag] = counts.get(r.tag, 0) + 1
    return dict(sorted(counts.items(), key=lambda x: -x[1]))


def clear() -> None:
    with _lock:
        _buf.clear()
        _event_state.clear()
        _candidate_state.clear()

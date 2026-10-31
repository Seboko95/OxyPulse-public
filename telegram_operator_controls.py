"""Verified Telegram bindings for production live-state controls."""
from __future__ import annotations

import os
from typing import Any, Mapping


def failed_required_preflight_checks(preflight: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Return the already-sanitized failed required checks from a preflight result."""
    diagnostics = preflight.get("diagnostics", {}) if isinstance(preflight, Mapping) else {}
    rows = diagnostics.get("failed_required_checks", []) if isinstance(diagnostics, Mapping) else []
    return [dict(row) for row in rows if isinstance(row, Mapping)]


def _markdown_safe_detail(detail: object) -> str:
    """Escape diagnostic text before inserting it into a Telegram Markdown response."""
    text = str(detail if detail not in (None, "") else "no detail")
    for character in ("\\", "`", "*", "_", "[", "]"):
        text = text.replace(character, f"\\{character}")
    return text


def format_preflight_failure(preflight: Mapping[str, Any], *, max_checks: int = 6) -> str:
    """Format operator-safe failure detail without reading raw check payloads."""
    diagnostics = preflight.get("diagnostics", {}) if isinstance(preflight, Mapping) else {}
    attempt_id = str(diagnostics.get("attempt_id", "unavailable")) if isinstance(diagnostics, Mapping) else "unavailable"
    failed = failed_required_preflight_checks(preflight)
    lines = [f"Attempt ID: `{attempt_id}`", "Failed required checks:"]
    for row in failed[:max_checks]:
        name = str(row.get("name", "unknown"))
        detail = _markdown_safe_detail(row.get("detail", "no detail"))
        lines.append(f"• `{name}` — {detail}")
    if len(failed) > max_checks:
        lines.append(f"• `{len(failed) - max_checks}` additional required checks omitted")
    if not failed:
        lines.append("• `unavailable` — no sanitized required-check detail was recorded")
    return "\n".join(lines)


async def arm(okx, engine=None, *, invocation_source: str = "telegram_operator_arm") -> dict[str, Any]:
    from live_preflight import run_preflight
    from live_state import state

    preflight = await run_preflight(
        okx,
        refresh_metadata=True,
        persist_state=False,
        invocation_source=invocation_source,
    )
    armed, reason = state.arm(preflight)
    snapshot = state.snapshot()
    effective = bool(armed and snapshot.get("armed") and snapshot.get("state") == "LIVE_ARMED")
    if effective and engine is not None:
        # An unarmed production startup deliberately latches the runtime into
        # research-only mode. ARM must clear that latch as part of the same
        # verified backend transition or submissions remain silently disabled.
        engine.research_only = False
        os.environ.pop("OXYPULSE_PRODUCTION_RESEARCH_ONLY", None)
    if effective:
        try:
            import okx_client as _okx_module
            _okx_module.mark_rest_arm_epoch()
        except Exception:
            pass
    runtime_execution_eligible = effective and not bool(getattr(engine, "research_only", False))
    return {
        "ok": runtime_execution_eligible,
        "action": "ARM",
        "reason": reason,
        "preflight": preflight,
        "state": snapshot,
        "runtime_execution_eligible": runtime_execution_eligible,
    }


async def disarm(engine, reason: str = "telegram_operator_disarm") -> dict[str, Any]:
    from live_state import state

    before_running = bool(engine.running)
    snapshot = state.disarm(reason)
    can_submit, submit_reason = state.can_submit_live()
    return {
        "ok": not can_submit and bool(engine.running) == before_running,
        "action": "DISARM",
        "state": snapshot,
        "can_submit_live": can_submit,
        "submit_reason": submit_reason,
        "engine_running": bool(engine.running),
        "position_management_preserved": True,
    }


async def halt(engine, reason: str = "telegram_operator_halt") -> dict[str, Any]:
    from live_state import state

    before_running = bool(engine.running)
    snapshot = state.halt(reason)
    can_submit, submit_reason = state.can_submit_live()
    return {
        "ok": not can_submit and bool(engine.running) == before_running,
        "action": "HALT",
        "state": snapshot,
        "can_submit_live": can_submit,
        "submit_reason": submit_reason,
        "engine_running": bool(engine.running),
        "position_management_preserved": True,
    }


async def emergency_close(engine) -> dict[str, Any]:
    from live_state import state

    snapshot = state.halt("telegram_emergency_close", emergency=True)
    close_result = await engine.close_all_positions()
    can_submit, submit_reason = state.can_submit_live()
    return {
        "ok": not can_submit,
        "action": "EMERGENCY_CLOSE",
        "state": snapshot,
        "can_submit_live": can_submit,
        "submit_reason": submit_reason,
        "close_result": close_result,
        "position_management_preserved": True,
    }

"""
OxyPulse - health_reporter.py
Periodic health/performance reporting with optional SMTP email delivery.
"""
from __future__ import annotations

import logging
import os
import re
import smtplib
import ssl
from datetime import datetime, timedelta
from email.message import EmailMessage
from pathlib import Path

import settings_store as _settings
from category_tracker import (
    ENGINE_EMOJI,
    ENGINE_NAMES,
    get_combined_stats,
    get_engine_stats,
    get_global_rolling_win_rate,
    get_rolling_win_rate,
)
from position_registry import get_all_details

logger = logging.getLogger("OxyPulse.Health")

REPORT_PATH = Path("data") / "monitor_report.md"
LOG_PATHS = [Path("logs") / "oxypulse.log", Path("logs") / "bot_err.log"]
_LOG_RE = re.compile(
    r"^(?P<ts>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}),\d+\s+\|\s+"
    r"(?P<level>[A-Z]+)\s+\|\s+(?P<logger>[^|]+)\s+\|\s+(?P<msg>.*)$"
)


def _recent_log_summary(window_hours: int = 4, max_examples: int = 5) -> dict:
    cutoff = datetime.now() - timedelta(hours=window_hours)
    errors: list[str] = []
    warnings = 0
    scanned = 0

    for path in LOG_PATHS:
        if not path.exists():
            continue
        try:
            lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
        except Exception as exc:
            logger.debug("Health log read failed for %s: %s", path, exc)
            continue

        for line in reversed(lines):
            scanned += 1
            match = _LOG_RE.match(line)
            if not match:
                continue
            try:
                ts = datetime.strptime(match.group("ts"), "%Y-%m-%d %H:%M:%S")
            except ValueError:
                continue
            if ts < cutoff:
                break
            level = match.group("level")
            msg = match.group("msg").strip()
            if level in {"ERROR", "CRITICAL"} and len(errors) < max_examples:
                errors.append(f"[{path.name}] {msg[:220]}")
            elif level == "WARNING":
                warnings += 1

    return {
        "errors": errors,
        "error_count": len(errors),
        "warnings": warnings,
        "scanned_lines": scanned,
    }


def _registry_issues() -> list[str]:
    issues: list[str] = []
    for coin, details in get_all_details().items():
        size = float(details.get("size_usdt", 0) or 0)
        entry = float(details.get("entry", 0) or 0)
        direction = str(details.get("direction", "") or "").lower()
        if size <= 0:
            issues.append(f"{coin}: non-positive size")
        if entry <= 0:
            issues.append(f"{coin}: missing entry")
        if direction not in {"long", "short"}:
            issues.append(f"{coin}: invalid direction={direction or 'blank'}")
    return issues


def _engine_table_lines() -> list[str]:
    rows: list[tuple[float, str]] = []
    for name in ENGINE_NAMES:
        stats = get_engine_stats(name)
        trades = int(stats.get("trade_count", 0) or 0)
        if trades == 0:
            continue
        roll = get_rolling_win_rate(name, 20)
        pnl = float(stats.get("total_pnl", 0.0) or 0.0)
        rows.append(
            (
                pnl,
                f"{ENGINE_EMOJI.get(name, '')} {name:<9} "
                f"roll20={roll['win_rate']:>5.1f}% "
                f"today={stats.get('today_wr', 0.0):>5.1f}% "
                f"all={stats.get('total_wr', 0.0):>5.1f}% "
                f"pnl=${pnl:+.2f} trades={trades}",
            )
        )
    rows.sort(key=lambda item: item[0], reverse=True)
    return [row for _pnl, row in rows] or ["No closed trades recorded yet."]


def build_health_report(
    *,
    engine_running: bool,
    running_count: int,
    total_engines: int,
    open_positions: int,
    sync_summary: str = "",
    window_hours: int = 4,
) -> tuple[str, str]:
    combined = get_combined_stats()
    rolling_20 = get_global_rolling_win_rate(20)
    rolling_50 = get_global_rolling_win_rate(50)
    log_summary = _recent_log_summary(window_hours=window_hours)
    registry_issues = _registry_issues()
    winrate_threshold = float(_settings.get_val("winrate_alert_threshold", 40.0))
    needs_attention = (
        bool(log_summary["error_count"])
        or bool(registry_issues)
        or (
            rolling_20["trades"] >= int(_settings.get_val("winrate_alert_min_trades", 10))
            and rolling_20["win_rate"] < winrate_threshold
        )
    )

    status = "ATTENTION" if needs_attention else "OK"
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    subject = (
        f"OxyPulse {status} | WR20 {rolling_20['win_rate']:.1f}% | "
        f"PnL ${combined['today_pnl']:+.2f} | {timestamp}"
    )

    lines = [
        f"OxyPulse 4h Health Report",
        f"Generated: {timestamp}",
        "",
        "Runtime",
        f"- Bot status: {'RUNNING' if engine_running else 'IDLE'}",
        f"- Engines running: {running_count}/{total_engines}",
        f"- Open positions: {open_positions}",
        "",
        "Performance",
        f"- Rolling 20 WR: {rolling_20['win_rate']:.1f}% ({rolling_20['wins']}W/{rolling_20['losses']}L)",
        f"- Rolling 50 WR: {rolling_50['win_rate']:.1f}% ({rolling_50['wins']}W/{rolling_50['losses']}L)",
        f"- Today: {combined['today_wins']}W/{combined['today_losses']}L | WR {combined['today_wr']:.1f}% | PnL ${combined['today_pnl']:+.2f}",
        f"- Week: {combined['week_wins']}W/{combined['week_losses']}L | WR {combined['week_wr']:.1f}% | PnL ${combined['week_pnl']:+.2f}",
        f"- All time: {combined['total_wins']}W/{combined['total_losses']}L | WR {combined['total_wr']:.1f}% | PnL ${combined['total_pnl']:+.2f}",
        f"- Best engine: {combined['best_engine']}",
        f"- Worst engine: {combined['worst_engine']}",
        "",
        "Sync",
        sync_summary.replace("`", "").replace("*", "") if sync_summary else "- No sync summary available.",
        "",
        "Health Checks",
        f"- Recent log errors: {log_summary['error_count']}",
        f"- Recent log warnings: {log_summary['warnings']}",
        f"- Registry issues: {len(registry_issues)}",
    ]

    if log_summary["errors"]:
        lines.extend(["", "Recent Errors"] + [f"- {item}" for item in log_summary["errors"]])

    if registry_issues:
        lines.extend(["", "Registry Issues"] + [f"- {item}" for item in registry_issues[:8]])

    lines.extend(["", "Engine Performance"] + [f"- {line}" for line in _engine_table_lines()])
    body = "\n".join(lines).strip() + "\n"
    return subject, body


def write_health_report(report_text: str, path: Path = REPORT_PATH) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(report_text, encoding="utf-8")
    return path


def send_email_report(subject: str, body: str, recipient: str | None = None) -> tuple[bool, str]:
    recipient = recipient or os.getenv("REPORT_EMAIL_TO", "").strip()
    smtp_host = os.getenv("REPORT_SMTP_HOST", "").strip()
    smtp_port = int(os.getenv("REPORT_SMTP_PORT", "465") or "465")
    smtp_user = os.getenv("REPORT_SMTP_USERNAME", "").strip()
    smtp_pass = os.getenv("REPORT_SMTP_PASSWORD", "").strip()
    from_addr = os.getenv("REPORT_EMAIL_FROM", "").strip() or smtp_user
    use_ssl = os.getenv("REPORT_SMTP_USE_SSL", "true").lower() == "true"

    missing = []
    if not recipient:
        missing.append("REPORT_EMAIL_TO")
    if not smtp_host:
        missing.append("REPORT_SMTP_HOST")
    if not smtp_user:
        missing.append("REPORT_SMTP_USERNAME")
    if not smtp_pass:
        missing.append("REPORT_SMTP_PASSWORD")
    if not from_addr:
        missing.append("REPORT_EMAIL_FROM")
    if missing:
        return False, "missing env: " + ", ".join(missing)

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = from_addr
    msg["To"] = recipient
    msg.set_content(body)

    context = ssl.create_default_context()
    try:
        if use_ssl:
            with smtplib.SMTP_SSL(smtp_host, smtp_port, context=context, timeout=30) as server:
                server.login(smtp_user, smtp_pass)
                server.send_message(msg)
        else:
            with smtplib.SMTP(smtp_host, smtp_port, timeout=30) as server:
                server.ehlo()
                server.starttls(context=context)
                server.ehlo()
                server.login(smtp_user, smtp_pass)
                server.send_message(msg)
    except Exception as exc:
        logger.warning("Health report email send failed: %s", exc)
        return False, str(exc)

    return True, recipient

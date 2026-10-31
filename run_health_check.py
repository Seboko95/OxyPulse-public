from __future__ import annotations

import argparse
import subprocess
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(dotenv_path=str(Path(__file__).resolve().parent / ".env"), override=True)

from category_tracker import ENGINE_NAMES
from health_reporter import build_health_report, send_email_report, write_health_report
from position_registry import get_all_details
import settings_store as _settings


def _bot_process_running() -> bool:
    root = str(Path(__file__).resolve().parent).lower()
    cmd = (
        "Get-CimInstance Win32_Process | "
        "Where-Object { $_.Name -like 'python*' -and $_.CommandLine } | "
        "Select-Object -ExpandProperty CommandLine"
    )
    try:
        out = subprocess.run(
            ["powershell", "-NoProfile", "-Command", cmd],
            capture_output=True,
            text=True,
            timeout=20,
            check=False,
        )
    except Exception:
        return False
    haystack = (out.stdout or "").lower()
    return "bot.py" in haystack and root in haystack


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the independent OxyPulse health check.")
    parser.add_argument("--email", action="store_true", help="Attempt SMTP email delivery.")
    args = parser.parse_args()

    enabled_count = sum(
        1 for name in ENGINE_NAMES
        if _settings.get_val(f"engine_{name.lower()}_enabled", True)
    )
    subject, body = build_health_report(
        engine_running=_bot_process_running(),
        running_count=enabled_count,
        total_engines=len(ENGINE_NAMES),
        open_positions=len(get_all_details()),
        sync_summary="Independent report - scheduler runs outside the bot process.",
        window_hours=4,
    )
    report_path = write_health_report(body)
    print(subject)
    print(f"report_path={report_path}")

    if args.email:
        ok, info = send_email_report(subject, body)
        print(f"email_sent={ok} detail={info}")
        return 0 if ok else 2

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

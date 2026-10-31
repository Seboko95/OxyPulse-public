from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from pathlib import Path

import pytest

from runtime_ownership import (
    RUNTIME_OWNERSHIP_EXIT_CODE,
    RuntimeOwnership,
    RuntimeOwnershipDenied,
)


ROOT = Path(__file__).resolve().parents[1]


def _python_process(code: str, *, stdin=subprocess.PIPE) -> subprocess.Popen:
    env = os.environ.copy()
    env["PYTHONPATH"] = str(ROOT) + os.pathsep + env.get("PYTHONPATH", "")
    return subprocess.Popen(
        [sys.executable, "-u", "-c", code],
        cwd=ROOT,
        env=env,
        stdin=stdin,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )


def _hold_code(lock_path: Path) -> str:
    return f"""
import json
import sys
from pathlib import Path
from runtime_ownership import RuntimeOwnership

owner = RuntimeOwnership(lock_path=Path({str(lock_path)!r}))
owner.acquire()
print(json.dumps(owner.status()), flush=True)
sys.stdin.read()
owner.release()
"""


def _try_code(lock_path: Path) -> str:
    return f"""
import json
import sys
from pathlib import Path
from runtime_ownership import RuntimeOwnership, RuntimeOwnershipDenied

try:
    owner = RuntimeOwnership(lock_path=Path({str(lock_path)!r}))
    owner.acquire()
except RuntimeOwnershipDenied as exc:
    print(json.dumps({{"denied": True, "message": str(exc)}}), flush=True)
    raise SystemExit({RUNTIME_OWNERSHIP_EXIT_CODE})
else:
    print(json.dumps({{"acquired": True, **owner.status()}}), flush=True)
    owner.release()
"""


def _readline(process: subprocess.Popen, timeout: float = 5.0) -> dict:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        line = process.stdout.readline()
        if line:
            return json.loads(line)
        time.sleep(0.01)
    raise AssertionError("subprocess did not publish ownership state")


def _release(process: subprocess.Popen) -> None:
    assert process.stdin is not None
    process.stdin.close()
    process.wait(timeout=5)
    assert process.returncode == 0


def test_first_instance_acquires_and_second_is_denied(tmp_path):
    lock_path = tmp_path / "runtime" / "data" / "oxypulse_runtime.lock"
    owner = _python_process(_hold_code(lock_path))
    try:
        first = _readline(owner)
        assert first["owned_by_this_process"] is True
        second = subprocess.run(
            [sys.executable, "-u", "-c", _try_code(lock_path)],
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=False,
        )
        assert second.returncode == RUNTIME_OWNERSHIP_EXIT_CODE
        denied = json.loads(second.stdout)
        assert denied["denied"] is True
        assert str(lock_path.resolve()) in denied["message"]
    finally:
        _release(owner)

    acquired = subprocess.run(
        [sys.executable, "-u", "-c", _try_code(lock_path)],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    assert acquired.returncode == 0
    assert json.loads(acquired.stdout)["acquired"] is True


def test_owner_lock_releases_after_subprocess_termination(tmp_path):
    lock_path = tmp_path / "data" / "oxypulse_runtime.lock"
    owner = _python_process(_hold_code(lock_path))
    _readline(owner)
    owner.kill()
    owner.wait(timeout=5)

    acquired = subprocess.run(
        [sys.executable, "-u", "-c", _try_code(lock_path)],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    assert acquired.returncode == 0
    assert json.loads(acquired.stdout)["acquired"] is True


def test_stale_file_contents_do_not_block_startup(tmp_path):
    lock_path = tmp_path / "data" / "oxypulse_runtime.lock"
    lock_path.parent.mkdir(parents=True)
    lock_path.write_text(
        json.dumps({"pid": 999999, "started_at": "2000-01-01T00:00:00+00:00"}),
        encoding="utf-8",
    )

    owner = RuntimeOwnership(lock_path=lock_path)
    owner.acquire()
    try:
        assert owner.status()["owned_by_this_process"] is True
    finally:
        owner.release()


def test_preexisting_zero_byte_lock_file_is_acquirable(tmp_path):
    lock_path = tmp_path / "data" / "oxypulse_runtime.lock"
    lock_path.parent.mkdir(parents=True)
    lock_path.touch()
    assert lock_path.stat().st_size == 0

    owner = RuntimeOwnership(lock_path=lock_path)
    owner.acquire()
    try:
        assert owner.owned is True
        assert lock_path.stat().st_size > 0
    finally:
        owner.release()


def test_metadata_is_diagnostic_and_never_controls_ownership(tmp_path):
    lock_path = tmp_path / "data" / "oxypulse_runtime.lock"
    owner = _python_process(_hold_code(lock_path))
    try:
        first = _readline(owner)
        assert first["owner_metadata"]["pid"] != 999999
        denied = subprocess.run(
            [sys.executable, "-u", "-c", _try_code(lock_path)],
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=False,
        )
        assert denied.returncode == RUNTIME_OWNERSHIP_EXIT_CODE
    finally:
        _release(owner)


def test_metadata_read_failure_does_not_bypass_duplicate_denial(tmp_path, monkeypatch):
    lock_path = tmp_path / "data" / "oxypulse_runtime.lock"
    owner = _python_process(_hold_code(lock_path))
    try:
        _readline(owner)
        metadata_failure_code = f"""
import json
import runtime_ownership as ownership_module
from pathlib import Path
from runtime_ownership import RUNTIME_OWNERSHIP_EXIT_CODE, RuntimeOwnership, RuntimeOwnershipDenied

ownership_module._read_metadata = lambda _path: {{}}
try:
    RuntimeOwnership(lock_path=Path({str(lock_path)!r})).acquire()
except RuntimeOwnershipDenied as exc:
    print(json.dumps({{"denied": True, "message": str(exc)}}), flush=True)
    raise SystemExit(RUNTIME_OWNERSHIP_EXIT_CODE)
else:
    raise SystemExit(0)
"""
        contender = subprocess.run(
            [sys.executable, "-u", "-c", metadata_failure_code],
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=False,
        )
        assert contender.returncode == RUNTIME_OWNERSHIP_EXIT_CODE
        denied = json.loads(contender.stdout)
        assert denied["denied"] is True
        assert str(lock_path.resolve()) in denied["message"]
    finally:
        _release(owner)


def test_metadata_failure_does_not_release_kernel_ownership(tmp_path, monkeypatch):
    lock_path = tmp_path / "data" / "oxypulse_runtime.lock"
    owner = RuntimeOwnership(lock_path=lock_path)

    def fail_metadata():
        raise OSError("metadata storage unavailable")

    monkeypatch.setattr(owner, "_write_metadata", fail_metadata)
    owner.acquire()
    try:
        assert owner.owned is True
        denied = subprocess.run(
            [sys.executable, "-u", "-c", _try_code(lock_path)],
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=False,
        )
        assert denied.returncode == RUNTIME_OWNERSHIP_EXIT_CODE
    finally:
        owner.release()


def test_release_is_idempotent_and_reacquisition_works(tmp_path):
    lock_path = tmp_path / "data" / "oxypulse_runtime.lock"
    owner = RuntimeOwnership(lock_path=lock_path)
    owner.acquire()
    owner.release()
    owner.release()
    assert owner.owned is False

    replacement = RuntimeOwnership(lock_path=lock_path)
    replacement.acquire()
    replacement.release()


def test_canonical_path_prevents_relative_path_bypass(tmp_path):
    root = tmp_path / "runtime"
    canonical = root / "data" / "oxypulse_runtime.lock"
    equivalent = root / "data" / "." / "nested" / ".." / "oxypulse_runtime.lock"
    first = RuntimeOwnership(lock_path=canonical)
    second = RuntimeOwnership(lock_path=equivalent)
    assert first.lock_path == second.lock_path == canonical.resolve()
    first.acquire()
    try:
        with pytest.raises(RuntimeOwnershipDenied):
            second.acquire()
    finally:
        first.release()
        second.release()


def test_default_lock_is_worktree_local():
    lock_path = RuntimeOwnership().lock_path
    assert lock_path == (ROOT / "data" / "oxypulse_runtime.lock").resolve()
    assert lock_path.parent.parent == ROOT.resolve()


def test_same_bot_absolute_path_from_unrelated_cwd_is_denied(tmp_path):
    lock_path = (ROOT / "data" / "oxypulse_runtime.lock").resolve()
    unrelated_cwd = tmp_path / "unrelated-cwd"
    unrelated_cwd.mkdir()
    owner = _python_process(_hold_code(lock_path))
    try:
        owner_status = _readline(owner)
        duplicate = subprocess.run(
            [sys.executable, "-u", str((ROOT / "bot.py").resolve())],
            cwd=unrelated_cwd,
            capture_output=True,
            text=True,
            check=False,
        )
        assert duplicate.returncode == RUNTIME_OWNERSHIP_EXIT_CODE
        assert str(lock_path) == owner_status["lock_path"]
        assert str(lock_path) in duplicate.stderr
        assert "telegram" not in duplicate.stderr.lower()
        assert "okx" not in duplicate.stderr.lower()
    finally:
        _release(owner)
        lock_path.unlink(missing_ok=True)


def test_worktree_roots_have_isolated_canonical_locks(tmp_path):
    root_a = tmp_path / "worktree-a"
    root_b = tmp_path / "worktree-b"
    first = RuntimeOwnership(root=root_a)
    second = RuntimeOwnership(root=root_b)
    assert first.lock_path != second.lock_path
    assert first.lock_path == (root_a / "data" / "oxypulse_runtime.lock").resolve()
    assert second.lock_path == (root_b / "data" / "oxypulse_runtime.lock").resolve()

    first.acquire()
    second.acquire()
    try:
        assert first.owned is True
        assert second.owned is True
    finally:
        first.release()
        second.release()


def test_bot_acquires_ownership_before_telegram_okx_and_engine_imports():
    source = (ROOT / "bot.py").read_text(encoding="utf-8")
    acquire = source.index("_runtime_ownership.acquire()")
    assert acquire < source.index("from dotenv import load_dotenv")
    assert acquire < source.index("from telegram import")
    assert acquire < source.index("from okx_client import")
    assert acquire < source.index("from trading_engine import")


def test_duplicate_bot_exits_before_initialization_and_preserves_live_state(tmp_path):
    lock_path = ROOT / "data" / "oxypulse_runtime.lock"
    runtime_state = {
        ROOT / "data" / "live_state.json": b'{"sentinel":"live-state"}\n',
        ROOT / "data" / "capital_reservations.json": b'{"sentinel":"reservation"}\n',
        ROOT / "data" / "order_intents.json": b'{"intents":[{"id":"sentinel-unresolved","state":"uncertain"}]}\n',
    }
    before = {}
    for path, payload in runtime_state.items():
        before[path] = path.read_bytes() if path.exists() else None
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(payload)
    owner = _python_process(_hold_code(lock_path))
    try:
        _readline(owner)
        duplicate = subprocess.run(
            [sys.executable, "bot.py"],
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=False,
        )
        assert duplicate.returncode == RUNTIME_OWNERSHIP_EXIT_CODE
        assert "runtime ownership denied" in duplicate.stderr.lower()
        assert "telegram" not in duplicate.stderr.lower()
        assert "okx" not in duplicate.stderr.lower()
        for path, payload in runtime_state.items():
            assert path.read_bytes() == payload
    finally:
        _release(owner)
        lock_path.unlink(missing_ok=True)
        for path, payload in before.items():
            if payload is None:
                path.unlink(missing_ok=True)
            else:
                path.write_bytes(payload)


def test_paper_and_live_flags_are_not_consulted_by_guard(tmp_path, monkeypatch):
    monkeypatch.setenv("PAPER_TRADING", "false")
    owner = RuntimeOwnership(lock_path=tmp_path / "data" / "lock")
    owner.acquire()
    try:
        assert owner.owned is True
    finally:
        owner.release()

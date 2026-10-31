"""Kernel-managed single-instance ownership for an OxyPulse runtime.

The lock file is only a stable kernel lock target. Its contents are bounded
diagnostic metadata and never determine ownership.
"""
from __future__ import annotations

import json
import logging
import os
import platform
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

LOCK_FILENAME = "oxypulse_runtime.lock"
RUNTIME_OWNERSHIP_EXIT_CODE = 73


class RuntimeOwnershipDenied(RuntimeError):
    """Raised when another process already owns the runtime lock."""

    def __init__(self, lock_path: Path, owner_metadata: dict[str, Any] | None = None) -> None:
        self.lock_path = Path(lock_path)
        self.owner_metadata = dict(owner_metadata or {})
        owner_pid = self.owner_metadata.get("pid")
        owner_started = self.owner_metadata.get("started_at")
        owner_workdir = self.owner_metadata.get("working_directory")
        details = []
        if owner_pid is not None:
            details.append(f"pid={owner_pid}")
        if owner_started:
            details.append(f"started_at={owner_started}")
        if owner_workdir:
            details.append(f"working_directory={owner_workdir}")
        suffix = f" ({', '.join(details)})" if details else ""
        super().__init__(
            "OxyPulse runtime ownership denied. Another instance owns "
            f"{self.lock_path}{suffix}"
        )


def _canonical_lock_path(root: os.PathLike[str] | str | None = None) -> Path:
    base = Path(__file__).resolve().parent if root is None else Path(root).resolve()
    return (base / "data" / LOCK_FILENAME).resolve()


def _read_metadata(lock_path: Path) -> dict[str, Any]:
    try:
        raw = lock_path.read_text(encoding="utf-8")
        value = json.loads(raw)
        return value if isinstance(value, dict) else {}
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        return {}


class RuntimeOwnership:
    """Own one canonical runtime lock for the lifetime of this object."""

    def __init__(
        self,
        root: os.PathLike[str] | str | None = None,
        *,
        lock_path: os.PathLike[str] | str | None = None,
    ) -> None:
        if lock_path is None:
            self.lock_path = _canonical_lock_path(root)
        else:
            self.lock_path = Path(lock_path).resolve()
        self._fd: int | None = None
        self._owner_metadata: dict[str, Any] = {}

    @property
    def owned(self) -> bool:
        return self._fd is not None

    def _open_lock_file(self) -> int:
        self.lock_path.parent.mkdir(parents=True, exist_ok=True)
        flags = os.O_RDWR | os.O_CREAT
        flags |= getattr(os, "O_BINARY", 0)
        fd = os.open(self.lock_path, flags, 0o600)
        try:
            if os.fstat(fd).st_size == 0:
                os.write(fd, b"{}")
            os.lseek(fd, 0, os.SEEK_SET)
            return fd
        except Exception:
            os.close(fd)
            raise

    @staticmethod
    def _lock(fd: int) -> None:
        if os.name == "nt":
            import msvcrt

            os.lseek(fd, 0, os.SEEK_SET)
            msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
            return

        import fcntl

        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)

    @staticmethod
    def _unlock(fd: int) -> None:
        if os.name == "nt":
            import msvcrt

            os.lseek(fd, 0, os.SEEK_SET)
            msvcrt.locking(fd, msvcrt.LK_UNLCK, 1)
            return

        import fcntl

        fcntl.flock(fd, fcntl.LOCK_UN)

    def _metadata(self) -> dict[str, Any]:
        metadata: dict[str, Any] = {
            "pid": os.getpid(),
            "ppid": os.getppid(),
            "started_at": datetime.now(timezone.utc).isoformat(),
            "hostname": platform.node(),
            "python_executable": str(Path(sys.executable).resolve()),
            "working_directory": str(Path.cwd().resolve()),
            "process_role": "oxypulse_bot",
        }
        try:
            import subprocess

            result = subprocess.run(
                ["git", "rev-parse", "HEAD"],
                cwd=Path(__file__).resolve().parent,
                capture_output=True,
                text=True,
                timeout=0.5,
                check=False,
            )
            head = result.stdout.strip()
            if result.returncode == 0 and head:
                metadata["git_head"] = head
        except Exception:
            pass
        return metadata

    def _write_metadata(self) -> None:
        if self._fd is None:
            return
        self._owner_metadata = self._metadata()
        payload = json.dumps(self._owner_metadata, separators=(",", ":"), sort_keys=True).encode("utf-8")
        os.ftruncate(self._fd, 0)
        os.lseek(self._fd, 0, os.SEEK_SET)
        os.write(self._fd, payload)
        os.fsync(self._fd)

    def acquire(self) -> "RuntimeOwnership":
        if self.owned:
            return self
        fd = self._open_lock_file()
        try:
            self._lock(fd)
        except (OSError, IOError) as exc:
            try:
                os.close(fd)
            finally:
                owner_metadata = _read_metadata(self.lock_path)
                raise RuntimeOwnershipDenied(self.lock_path, owner_metadata) from exc

        self._fd = fd
        try:
            self._write_metadata()
        except Exception as exc:
            logger.warning("Runtime ownership metadata write failed: %s", exc)
        return self

    def release(self) -> None:
        fd = self._fd
        self._fd = None
        if fd is None:
            return
        try:
            self._unlock(fd)
        except (OSError, IOError):
            logger.debug("Runtime ownership unlock failed during release", exc_info=True)
        finally:
            try:
                os.close(fd)
            except OSError:
                pass

    def status(self) -> dict[str, Any]:
        metadata = self._owner_metadata if self.owned else _read_metadata(self.lock_path)
        return {
            "owned_by_this_process": self.owned,
            "lock_path": str(self.lock_path),
            "owner_pid": metadata.get("pid"),
            "owner_metadata": metadata,
        }

    def __enter__(self) -> "RuntimeOwnership":
        return self.acquire()

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.release()


def status(lock_path: os.PathLike[str] | str | None = None) -> dict[str, Any]:
    """Return bounded diagnostic ownership information without acquiring."""
    return RuntimeOwnership(lock_path=lock_path).status() if lock_path else RuntimeOwnership().status()

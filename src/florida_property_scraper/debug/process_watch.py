from __future__ import annotations

import atexit
import json
import os
import signal
import sys
import threading
import traceback
from datetime import datetime, timezone
from pathlib import Path


_INSTALLED = False


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _log_path() -> Path:
    base = _repo_root() / ".logs"
    try:
        base.mkdir(parents=True, exist_ok=True)
    except Exception:
        pass
    return base / "backend_signals.log"


def _write_event(payload: dict) -> None:
    try:
        line = json.dumps(payload, ensure_ascii=False, default=str)
        path = _log_path()
        with open(path, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        return


def _signal_name(signum: int) -> str:
    try:
        return signal.Signals(signum).name
    except Exception:
        return f"SIGNAL_{signum}"


def _handle_signal(signum: int, frame) -> None:
    try:
        stack = "".join(traceback.format_stack(frame)) if frame is not None else ""
    except Exception:
        stack = ""

    if len(stack) > 64000:
        stack = stack[-64000:]

    payload = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "signal": _signal_name(signum),
        "pid": os.getpid(),
        "ppid": os.getppid(),
        "thread": threading.current_thread().name,
        "stack": stack,
        "argv": list(sys.argv),
    }
    _write_event(payload)


def _handle_exit() -> None:
    payload = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "event": "atexit",
        "pid": os.getpid(),
        "ppid": os.getppid(),
    }
    _write_event(payload)


def install_process_watch() -> None:
    global _INSTALLED
    if _INSTALLED:
        return
    _INSTALLED = True

    for name in ("SIGTERM", "SIGINT", "SIGHUP", "SIGQUIT"):
        sig = getattr(signal, name, None)
        if sig is None:
            continue
        try:
            signal.signal(sig, _handle_signal)
        except Exception:
            continue

    try:
        atexit.register(_handle_exit)
    except Exception:
        pass
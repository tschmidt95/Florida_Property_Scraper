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
        line = json.dumps(payload, ensure_ascii=True, default=str)
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


def _read_proc_cmdline(pid: int | None) -> str:
    if not pid:
        return ""
    try:
        path = Path(f"/proc/{int(pid)}/cmdline")
        raw = path.read_bytes()
        if not raw:
            return ""
        return " ".join([p.decode("utf-8", errors="replace") for p in raw.split(b"\0") if p])
    except Exception:
        return ""


def _process_metadata() -> dict:
    pid = os.getpid()
    ppid = os.getppid()
    try:
        pgid = os.getpgid(pid)
    except Exception:
        pgid = None
    try:
        sid = os.getsid(pid)
    except Exception:
        sid = None
    return {
        "pid": pid,
        "ppid": ppid,
        "pgid": pgid,
        "sid": sid,
        "argv": list(sys.argv),
        "ppid_cmdline": _read_proc_cmdline(ppid),
    }


def _emit_event(event: str, extra: dict | None = None) -> None:
    payload = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "event": event,
        **_process_metadata(),
    }
    if extra:
        payload.update(extra)
    _write_event(payload)


def _handle_signal(signum: int, frame) -> None:
    try:
        stack = "".join(traceback.format_stack(frame)) if frame is not None else ""
    except Exception:
        stack = ""

    if len(stack) > 64000:
        stack = stack[-64000:]

    _emit_event(
        "signal",
        {
            "signal": _signal_name(signum),
            "signal_num": int(signum),
            "thread": threading.current_thread().name,
            "stack": stack,
        },
    )


def _handle_exit() -> None:
    _emit_event("atexit")


def record_shutdown_event(reason: str = "shutdown") -> None:
    _emit_event("shutdown", {"reason": str(reason)})


def install_process_watch() -> None:
    global _INSTALLED
    if _INSTALLED:
        return
    _INSTALLED = True

    try:
        _log_path().touch(exist_ok=True)
    except Exception:
        pass

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

    _emit_event("watch_installed")